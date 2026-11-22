export * as SpaceStore from "./space-store"

import { Flag } from "@opencode/core/flag"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-services"
import { AbsolutePath } from "@opencode/core/schema"
import { Space } from "@opencode/core/space"
import { Global } from "@opencode/util/global"
import { Database } from "@opencode/core/database/database"
import { KeyedMutex } from "@opencode/core/effect/keyed-mutex"
import { Space as SpaceSchema } from "@opencode/schema/space"
import { Event } from "@opencode/schema/event"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Clock, Context, Effect, Layer, Stream } from "effect"
import { EventFeed } from "./event-feed"

/** Whether the `spaces` flag is on in the server's base configuration: spaces are server-wide, not per location. */
export const enabled = Effect.gen(function* () {
  const global = yield* Global.Service
  const locations = yield* LocationServiceMap.Service
  return Flag.enabled("spaces").pipe(
    Effect.provide(locations.get(Location.Ref.make({ directory: AbsolutePath.make(global.config) }))),
    Effect.orElseSucceed(() => false),
  )
})

/** Rows per snapshot or delta batch, so a large space never goes out as one event. */
export const BatchSize = 500

export interface Request {
  readonly space: SpaceSchema.Name
  /** The epoch and last revision the client applied; a delta is sent only when this database issued them. */
  readonly epoch?: string
  readonly since?: number
}

export type Initial =
  | {
      readonly type: typeof SpaceSchema.Event.Snapshot.type
      readonly data: typeof SpaceSchema.Event.Snapshot.data.Type
    }
  | { readonly type: typeof SpaceSchema.Event.Delta.type; readonly data: typeof SpaceSchema.Event.Delta.data.Type }

export type Change = typeof SpaceSchema.Event.Updated.Type | typeof SpaceSchema.Event.Deleted.Type
export type Listener = (change: Change) => Effect.Effect<void>

export interface Interface {
  readonly set: (space: SpaceSchema.Name, key: SpaceSchema.Key, value: SpaceSchema.Value) => Effect.Effect<void>
  readonly remove: (space: SpaceSchema.Name, key: SpaceSchema.Key) => Effect.Effect<void>
  /** This database's epoch, created once and kept, so revisions from another server or database are never used. */
  readonly epoch: string
  /** Replaces the epoch: every client reads a fresh snapshot on its next subscribe, e.g. after a database restore. */
  readonly resetEpoch: Effect.Effect<void>
  /**
   * The space's state in batches of `BatchSize` rows: deltas since `since` when this database issued it and still
   * holds every deletion after it, else a snapshot followed by deltas. A snapshot also replaces the deltas when
   * deletions they need are purged while they go out. Each batch covers every change up to its revision.
   */
  readonly read: (request: Request) => Stream.Stream<Initial>
  /**
   * Runs `listener` with every change before its write returns, in revision order within a space. Changes never go
   * through the bus, so plugins do not see them; only `extension` listens.
   */
  readonly listen: (listener: Listener) => Effect.Effect<Effect.Effect<void>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/server/SpaceStore") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    // Without a stored epoch, this process's own one still keeps cursors safe: they only fall back to snapshots.
    let epoch = yield* Space.epoch(db).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not read the space epoch", { cause }).pipe(Effect.as(crypto.randomUUID())),
      ),
    )
    const listeners = new Array<Listener>()
    // Held per space from the commit until listeners have the change, so they get each space in revision order.
    // Reads and other spaces never wait on it.
    const locks = KeyedMutex.makeUnsafe<SpaceSchema.Name>()

    const commit = (space: SpaceSchema.Name, write: Effect.Effect<Change["data"] | undefined>) =>
      locks.withLock(space)(
        // A dropped request must not commit a change that listeners never hear about.
        Effect.uninterruptible(
          Effect.gen(function* () {
            const data = yield* write
            if (!data) return
            const change = {
              id: Event.ID.create(),
              created: yield* Clock.currentTimeMillis,
              ...("value" in data
                ? { type: SpaceSchema.Event.Updated.type, data }
                : { type: SpaceSchema.Event.Deleted.type, data }),
            }
            yield* Effect.forEach(listeners, (listener) => listener(change), { discard: true })
          }),
        ),
      )

    // Pages the live rows up to the head it starts from, so it reads no tombstones, and the last page reports that
    // head. A key written while the pages go out moves above it, and a delta from there brings the change.
    const snapshot = (space: SpaceSchema.Name): Stream.Stream<Initial> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const head = yield* Space.head(db, space)
          const page = (since: SpaceSchema.Revision, first: boolean): Stream.Stream<Initial> =>
            Stream.unwrap(
              Effect.gen(function* () {
                const rows = yield* Space.live(db, space, since, head, BatchSize)
                const last = rows.length < BatchSize
                const data = {
                  space,
                  epoch,
                  revision: last ? head : (rows.at(-1)?.revision ?? head),
                  entries: entries(rows),
                }
                const item: Initial = first
                  ? { type: SpaceSchema.Event.Snapshot.type, data }
                  : { type: SpaceSchema.Event.Delta.type, data: { ...data, deleted: [] } }
                return Stream.make(item).pipe(
                  Stream.concat(last ? delta(space, head, true) : page(data.revision, false)),
                )
              }),
            )
          return page(0, true)
        }),
      )

    // Pages the rows after `since` through the revision index, so a key rewritten meanwhile moves to a later page and
    // every page's revision covers all changes up to it. A page whose deletions may have been purged before it was
    // read is replaced by a snapshot.
    const delta = (space: SpaceSchema.Name, since: SpaceSchema.Revision, skipEmpty: boolean): Stream.Stream<Initial> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const rows = yield* Space.changes(db, space, since, BatchSize)
          // Read after the rows: a purge above `since` the rows may have missed has raised the floor by now.
          if ((yield* Space.floor(db, space)) > since) return snapshot(space)
          if (skipEmpty && !rows.length) return Stream.empty
          const revision = rows.at(-1)?.revision ?? since
          const item: Initial = {
            type: SpaceSchema.Event.Delta.type,
            data: {
              space,
              epoch,
              revision,
              entries: entries(rows),
              deleted: rows.flatMap((row) => (row.value === null ? [row.key] : [])),
            },
          }
          return Stream.make(item).pipe(
            Stream.concat(rows.length < BatchSize ? Stream.empty : delta(space, revision, false)),
          )
        }),
      )

    return Service.of({
      set: Effect.fn("SpaceStore.set")(function* (space, key, value) {
        yield* commit(
          space,
          Space.set(db, space, key, value).pipe(Effect.map((revision) => ({ space, key, value, revision }))),
        )
      }),
      remove: Effect.fn("SpaceStore.remove")(function* (space, key) {
        yield* commit(
          space,
          Space.remove(db, space, key).pipe(
            Effect.map((revision) => (revision === undefined ? undefined : { space, key, revision })),
          ),
        )
      }),
      get epoch() {
        return epoch
      },
      resetEpoch: Effect.fn("SpaceStore.resetEpoch")(function* () {
        epoch = yield* Space.resetEpoch(db)
      })(),
      read: (request) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const since = request.epoch === epoch ? request.since : undefined
            return since !== undefined && since <= (yield* Space.head(db, request.space))
              ? delta(request.space, since, false)
              : snapshot(request.space)
          }),
        ),
      listen: (listener) =>
        Effect.sync(() => {
          listeners.push(listener)
          return Effect.sync(() => {
            const index = listeners.indexOf(listener)
            if (index >= 0) listeners.splice(index, 1)
          })
        }),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [Database.node] })

/**
 * Serves `experimental.space` on the event stream: each requested space's snapshot or delta, then its later changes,
 * none missed or repeated.
 */
export const extension = Effect.flatMap(enabled, (on) => extensionWhen(on))

/** `extension`, serving spaces only while `on` is true. */
export const extensionWhen = Effect.fnUntraced(function* (on: Effect.Effect<boolean>) {
  const store = yield* Service
  const streams = new Set<Listener>()
  const unlisten = yield* store.listen((change) =>
    Effect.forEach(streams, (stream) => stream(change), { discard: true }),
  )
  yield* Effect.addFinalizer(() => unlisten)
  return {
    open: (query, send) =>
      Effect.gen(function* () {
        const requests = new Map(
          (query["experimental.space"] ?? []).map(parse).map((request) => [request.space, request]),
        )
        if (!requests.size || !(yield* on)) return []
        // The revision each space's initial state covers. Until it is read, changes wait in `pending`: the read may
        // already hold them.
        const covered = new Map<SpaceSchema.Name, SpaceSchema.Revision>()
        let pending: Array<Change> | undefined = []
        const forward = (change: Change) =>
          change.data.revision > (covered.get(change.data.space) ?? 0) ? send(change) : Effect.void
        const stream: Listener = (change) =>
          Effect.suspend(() => {
            if (!requests.has(change.data.space)) return Effect.void
            if (!pending) return forward(change)
            pending.push(change)
            return Effect.void
          })
        streams.add(stream)
        yield* Effect.addFinalizer(() => Effect.sync(() => streams.delete(stream)))
        const initial = yield* Stream.fromIterable(requests.values()).pipe(
          Stream.flatMap((request) => store.read(request)),
          Stream.runCollect,
        )
        for (const item of initial) covered.set(item.data.space, item.data.revision)
        // Changes arriving while these go out join the end, so each space stays in revision order.
        for (let change = pending.shift(); change; change = pending.shift()) yield* forward(change)
        pending = undefined
        const created = yield* Clock.currentTimeMillis
        return Array.from(initial, (item) => ({ id: Event.ID.create(), created, ...item }))
      }),
  } satisfies EventFeed.Extension
})

function entries(rows: ReadonlyArray<{ readonly key: SpaceSchema.Key; readonly value: SpaceSchema.Value | null }>) {
  return rows.flatMap((row) => (row.value === null ? [] : [{ key: row.key, value: row.value }]))
}

// `space:epoch.revision`; a cursor that does not parse is unusable, so the space gets a snapshot.
function parse(item: string): Request {
  const index = item.indexOf(":")
  if (index < 0) return { space: item }
  const space = item.slice(0, index)
  const cursor = item.slice(index + 1)
  const dot = cursor.lastIndexOf(".")
  const revision = cursor.slice(dot + 1)
  if (dot < 1 || !/^\d+$/u.test(revision) || !Number.isSafeInteger(Number(revision))) return { space }
  return { space, epoch: cursor.slice(0, dot), since: Number(revision) }
}
