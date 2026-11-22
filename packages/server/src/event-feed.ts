export * as EventFeed from "./event-feed"

import { Bus } from "@opencode/core/bus"
import { Event } from "@opencode/schema/event"
import { isOpenCodeEvent, type EventSubscribeQuery, type OpenCodeEvent } from "@opencode/protocol/groups/event"
import { Cause, Context, Effect, Exit, Layer, Queue, Schema, Scope, Stream } from "effect"

export const SubscriberCapacity = 4_096

export class SubscriberOverflowError extends Schema.TaggedError<SubscriberOverflowError>()(
  "EventFeed.SubscriberOverflow",
  { capacity: Schema.Int },
) {}

export class EncodingError extends Schema.TaggedError<EncodingError>()("EventFeed.EncodingError", {
  eventID: Event.ID,
  eventType: Schema.String,
  cause: Schema.Defect(),
}) {}

export type Error = SubscriberOverflowError | EncodingError

export interface Interface {
  readonly subscribe: (query?: EventSubscribeQuery) => Effect.Effect<Stream.Stream<string, Error>, never, Scope.Scope>
}

/**
 * Adds events from outside the bus to the streams whose query asks for them. `open` runs for each stream once it is
 * registered: an event given to its `send` from then on reaches that stream, after the events `open` returns.
 * `types` does not apply to them, and the extension itself picks the streams and drops repeats.
 */
export interface Extension {
  readonly open: (
    query: EventSubscribeQuery,
    send: (event: OpenCodeEvent) => Effect.Effect<void>,
  ) => Effect.Effect<ReadonlyArray<OpenCodeEvent>, never, Scope.Scope>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/server/EventFeed") {}

export function frame(event: OpenCodeEvent) {
  return `data: ${JSON.stringify(event)}\n\n`
}

export const make = Effect.fn("EventFeed.make")(function* (
  observe: (subscriber: Bus.Subscriber) => Effect.Effect<Bus.Unsubscribe>,
  options?: {
    readonly capacity?: number
    readonly encode?: (event: OpenCodeEvent) => string
    readonly extensions?: ReadonlyArray<Extension>
  },
) {
  const capacity = options?.capacity ?? SubscriberCapacity
  const render = options?.encode ?? frame
  const subscribers = new Map<Queue.Queue<string, Error>, ReadonlySet<string> | undefined>()

  const fail = (error: Error) =>
    Effect.sync(() => {
      const current = Array.from(subscribers.keys())
      subscribers.clear()
      for (const subscriber of current) Queue.failCauseUnsafe(subscriber, Cause.fail(error))
    })

  const encode = (event: OpenCodeEvent) =>
    Effect.try({
      try: () => render(event),
      catch: (cause) => new EncodingError({ eventID: event.id, eventType: event.type, cause }),
    }).pipe(
      Effect.catch((error) =>
        Effect.logError("Failed to encode public event", {
          eventID: error.eventID,
          eventType: error.eventType,
          cause: error.cause,
        }).pipe(Effect.andThen(fail(error)), Effect.as(undefined)),
      ),
    )

  const offer = (subscriber: Queue.Queue<string, Error>, encoded: string) => {
    if (Queue.offerUnsafe(subscriber, encoded)) return
    subscribers.delete(subscriber)
    Queue.failCauseUnsafe(subscriber, Cause.fail(new SubscriberOverflowError({ capacity })))
  }

  const publish = Effect.fnUntraced(function* (event: Event.Payload) {
    if (!isOpenCodeEvent(event)) return
    let encoded: string | undefined
    for (const [subscriber, types] of subscribers) {
      if (types && !types.has(event.type)) continue
      encoded ??= yield* encode(event)
      if (encoded === undefined) return
      offer(subscriber, encoded)
    }
  })

  const unsubscribe = yield* observe(publish)
  yield* Effect.addFinalizer(() => unsubscribe)

  return Service.of({
    subscribe: (query = {}) =>
      Effect.gen(function* () {
        const queue = yield* Effect.acquireRelease(
          Queue.dropping<string, Error>(capacity).pipe(
            Effect.tap((queue) =>
              Effect.sync(() => subscribers.set(queue, query.types?.length ? new Set(query.types) : undefined)),
            ),
          ),
          (queue) =>
            Effect.sync(() => subscribers.delete(queue)).pipe(Effect.andThen(Queue.shutdown(queue)), Effect.asVoid),
        )
        // An extension never fails the stream: an event it cannot encode is dropped, and a failed `open` only loses
        // that extension's events for this stream.
        const send = (event: OpenCodeEvent) =>
          Effect.suspend(() => {
            if (!subscribers.has(queue)) return Effect.void
            return Effect.try(() => render(event)).pipe(
              Effect.flatMap((encoded) => Effect.sync(() => offer(queue, encoded))),
              Effect.catchCause((cause) =>
                Effect.logError("Failed to encode extension event", { type: event.type, cause }),
              ),
            )
          })
        const scope = yield* Scope.Scope
        const initial = (yield* Effect.forEach(options?.extensions ?? [], (extension) =>
          Effect.gen(function* () {
            const child = yield* Scope.fork(scope)
            return yield* extension.open(query, send).pipe(
              Scope.provide(child),
              Effect.catchCause((cause) =>
                Effect.logError("Event stream extension failed", { cause }).pipe(
                  Effect.andThen(Scope.close(child, Exit.void)),
                  Effect.as([]),
                ),
              ),
            )
          }),
        )).flat()
        const live = Stream.fromQueue(queue)
        return initial.length ? Stream.fromIterable(initial).pipe(Stream.map(render), Stream.concat(live)) : live
      }),
  })
})

export const layer = <R>(...extensions: ReadonlyArray<Effect.Effect<Extension, never, R>>) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      return yield* make(bus.listen, { extensions: yield* Effect.all(extensions) })
    }),
  )
