import { expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { Space } from "@opencode/core/space"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Context, Deferred, Effect, Fiber, Layer, Stream } from "effect"
import { it } from "../../core/test/lib/effect"
import { SpaceStore } from "../src/space-store"
import { EventFeed } from "../src/event-feed"

// One in-memory database shared by every `boot()`, each a fresh store, bus and feed as after a process restart.
// `hold(revision)` pauses the change with that revision after it commits and before the feed hears of it, so a test
// can line up competing writes and subscriptions behind it.
const setup = Effect.gen(function* () {
  const database = Context.get(yield* Layer.build(LayerNode.compile(Database.node)), Database.Service)
  const shared = Database.node.replace(Layer.succeed(Database.Service, database))
  const gates = new Map<number, { reached: Deferred.Deferred<void>; release: Deferred.Deferred<void> }>()
  const boot = Effect.fnUntraced(function* (wrap?: (store: SpaceStore.Interface) => SpaceStore.Interface) {
    const bus = Context.get(yield* Layer.build(LayerNode.compile(Bus.node, { replacements: [shared] })), Bus.Service)
    const store = Context.get(
      yield* Layer.build(LayerNode.compile(SpaceStore.node, { replacements: [shared] })),
      SpaceStore.Service,
    )
    // Registered before the feed, so it runs first.
    yield* store.listen((change) =>
      Effect.suspend(() => {
        const gate = gates.get(change.data.revision)
        if (!gate) return Effect.void
        gates.delete(change.data.revision)
        return Deferred.succeed(gate.reached, undefined).pipe(Effect.andThen(Deferred.await(gate.release)))
      }),
    )
    const extension = yield* SpaceStore.extensionWhen(Effect.succeed(true)).pipe(
      Effect.provideService(SpaceStore.Service, wrap ? wrap(store) : store),
    )
    return { store, bus, feed: yield* EventFeed.make(bus.listen, { extensions: [extension] }) }
  })
  const hold = Effect.fnUntraced(function* (revision: number) {
    const gate = { reached: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
    gates.set(revision, gate)
    return gate
  })
  return { boot, hold, db: database.db }
})

const take = (live: Stream.Stream<string, EventFeed.Error>, count: number) =>
  live.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map((frames) =>
      Array.from(frames, (frame) => {
        const event = JSON.parse(frame.slice("data: ".length))
        return { type: event.type, data: event.data }
      }),
    ),
  )

const now = { startImmediately: true } as const

const collect = (store: SpaceStore.Interface, requests: ReadonlyArray<SpaceStore.Request>) =>
  Stream.fromIterable(requests).pipe(
    Stream.flatMap((request) => store.read(request)),
    Stream.runCollect,
    Effect.map((items) => Array.from(items)),
  )

// What a client holds after applying a batch: a snapshot replaces everything, a delta changes and deletes keys.
const apply = (
  state: Map<string, unknown>,
  batch: {
    readonly type: string
    readonly data: {
      readonly entries: ReadonlyArray<{ readonly key: string; readonly value: unknown }>
      readonly deleted?: ReadonlyArray<string>
    }
  },
) => {
  if (batch.type === "experimental.space.snapshot") state.clear()
  for (const entry of batch.data.entries) state.set(entry.key, entry.value)
  for (const key of batch.data.deleted ?? []) state.delete(key)
  return state
}

const range = (count: number) => Array.from({ length: count }, (_, index) => index)

it.live("delivers concurrent writes to one space in revision order", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store, feed } = yield* boot()
    const live = yield* feed.subscribe({ "experimental.space": ["app"] })
    const writes = Array.from({ length: 20 }, (_, index) => index)

    yield* Effect.forEach(
      writes,
      (index) =>
        Effect.all([store.set("app", `k${index % 4}`, index), store.set(`space-${index % 3}`, "k", index)], {
          concurrency: "unbounded",
        }),
      { concurrency: "unbounded", discard: true },
    )

    const [snapshot, ...changes] = yield* take(live, writes.length + 1)
    expect(snapshot).toEqual({
      type: "experimental.space.snapshot",
      data: { space: "app", epoch: store.epoch, revision: 0, entries: [] },
    })
    expect(changes.map((change) => change.data)).toEqual(
      writes.map((_, index) => expect.objectContaining({ space: "app", revision: index + 1 })),
    )
    // The last change of each key is the value read back.
    const last = new Map(changes.map((change) => [change.data.key, change.data.value]))
    const [state] = yield* collect(store, [{ space: "app" }])
    expect(new Map(state.data.entries.map((entry) => [entry.key, entry.value]))).toEqual(last)
  }),
)

it.live("a write held before delivery blocks neither subscribers, readers nor other spaces", () =>
  Effect.gen(function* () {
    const { boot, hold } = yield* setup
    const { store, feed } = yield* boot()
    yield* store.set("app", "k", "old")
    yield* store.set("app", "gone", 1)

    const pending = yield* hold(3)
    const write = yield* store.set("app", "k", "new").pipe(Effect.forkChild(now))
    yield* Deferred.await(pending.reached)
    // "new" (revision 3) is committed but not yet delivered, and the space's lock is held.
    const fresh = yield* feed.subscribe({ "experimental.space": ["app"] })
    const resumed = yield* feed.subscribe({ "experimental.space": [`app:${store.epoch}.2`] })
    yield* store.set("other", "x", 1)
    const next = yield* store.set("app", "k", "newer").pipe(Effect.forkChild(now))
    yield* Deferred.succeed(pending.release, undefined)
    yield* Fiber.join(write)
    yield* Fiber.join(next)
    yield* store.remove("app", "k")

    // Both registered before "new" was delivered and read it in their initial state; it is not sent twice.
    const after = [
      { type: "experimental.space.updated", data: { space: "app", key: "k", value: "newer", revision: 4 } },
      { type: "experimental.space.deleted", data: { space: "app", key: "k", revision: 5 } },
    ]
    expect(yield* take(fresh, 3)).toEqual([
      {
        type: "experimental.space.snapshot",
        data: {
          space: "app",
          epoch: store.epoch,
          revision: 3,
          entries: [
            { key: "gone", value: 1 },
            { key: "k", value: "new" },
          ],
        },
      },
      ...after,
    ])
    expect(yield* take(resumed, 3)).toEqual([
      {
        type: "experimental.space.delta",
        data: { space: "app", epoch: store.epoch, revision: 3, entries: [{ key: "k", value: "new" }], deleted: [] },
      },
      ...after,
    ])
  }),
)

it.live("writes on either side of a subscription's read reach it exactly once", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store, feed } = yield* boot((store) => ({
      ...store,
      read: (request) =>
        Stream.unwrap(store.set("app", "k", "before read").pipe(Effect.as(store.read(request)))).pipe(
          Stream.concat(Stream.fromEffect(store.set("app", "k", "after read")).pipe(Stream.drain)),
        ),
    }))
    const live = yield* feed.subscribe({ "experimental.space": ["app"] })
    yield* store.set("app", "k", "later")

    expect(yield* take(live, 3)).toEqual([
      {
        type: "experimental.space.snapshot",
        data: { space: "app", epoch: store.epoch, revision: 1, entries: [{ key: "k", value: "before read" }] },
      },
      { type: "experimental.space.updated", data: { space: "app", key: "k", value: "after read", revision: 2 } },
      { type: "experimental.space.updated", data: { space: "app", key: "k", value: "later", revision: 3 } },
    ])
  }),
)

it.live("catches each space up from its revision: final values and deletions, else a snapshot", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store } = yield* boot()
    yield* store.set("app", "kept", "v1")
    yield* store.set("app", "churn", 1)
    yield* store.set("app", "doomed", true)
    yield* store.set("other", "x", "y")

    // While the client is away: repeated updates, a delete, a delete repeated, and a delete then re-set.
    yield* store.set("app", "churn", 2)
    yield* store.set("app", "churn", 3)
    yield* store.remove("app", "doomed")
    yield* store.remove("app", "doomed")
    yield* store.remove("app", "never-set")
    yield* store.set("app", "back", "a")
    yield* store.remove("app", "back")
    yield* store.set("app", "back", "b")

    const epoch = store.epoch
    expect(
      yield* collect(store, [
        { space: "app", epoch, since: 3 },
        { space: "other", epoch, since: 1 },
        { space: "fresh" },
        { space: "ahead", epoch, since: 9 },
        { space: "zero", epoch, since: 0 },
        { space: "app", epoch, since: 9 },
        { space: "app", epoch: "another-database", since: 9 },
      ]),
    ).toEqual([
      {
        type: "experimental.space.delta",
        data: {
          space: "app",
          epoch,
          revision: 9,
          entries: [
            { key: "churn", value: 3 },
            { key: "back", value: "b" },
          ],
          deleted: ["doomed"],
        },
      },
      { type: "experimental.space.delta", data: { space: "other", epoch, revision: 1, entries: [], deleted: [] } },
      { type: "experimental.space.snapshot", data: { space: "fresh", epoch, revision: 0, entries: [] } },
      { type: "experimental.space.snapshot", data: { space: "ahead", epoch, revision: 0, entries: [] } },
      { type: "experimental.space.delta", data: { space: "zero", epoch, revision: 0, entries: [], deleted: [] } },
      { type: "experimental.space.delta", data: { space: "app", epoch, revision: 9, entries: [], deleted: [] } },
      {
        type: "experimental.space.snapshot",
        data: {
          space: "app",
          epoch,
          revision: 9,
          entries: [
            { key: "kept", value: "v1" },
            { key: "churn", value: 3 },
            { key: "back", value: "b" },
          ],
        },
      },
    ])
  }),
)

it.live("a restarted store resumes revisions and catch-up from storage alone", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const before = yield* boot()
    yield* before.store.set("app", "k", "a")
    yield* before.store.set("app", "k", "b")
    yield* before.store.remove("app", "k")

    // A new process over the same database. Its first write must not reuse a revision, even the tombstone's.
    const after = yield* boot()
    expect(after.store.epoch).toBe(before.store.epoch)
    yield* after.store.set("app", "j", "c")
    const live = yield* after.feed.subscribe({ "experimental.space": [`app:${after.store.epoch}.1`] })
    yield* after.store.set("app", "k", "d")
    expect(yield* take(live, 2)).toEqual([
      {
        type: "experimental.space.delta",
        data: {
          space: "app",
          epoch: after.store.epoch,
          revision: 4,
          entries: [{ key: "j", value: "c" }],
          deleted: ["k"],
        },
      },
      { type: "experimental.space.updated", data: { space: "app", key: "k", value: "d", revision: 5 } },
    ])
  }),
)

it.live("sends space events only to subscribers of their space and leaves other events alone", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store, feed, bus } = yield* boot()
    const app = yield* feed.subscribe({ types: [Agent.Event.Updated.type], "experimental.space": ["app"] })
    const other = yield* feed.subscribe({ "experimental.space": ["other"] })
    const plain = yield* feed.subscribe()
    const seen: Array<string> = []
    yield* bus.listen((event) => Effect.sync(() => seen.push(event.type)))

    yield* store.set("app", "k", { n: 1 })
    yield* bus.publish(Agent.Event.Updated, {})

    expect(yield* take(app, 3)).toEqual([
      { type: "experimental.space.snapshot", data: { space: "app", epoch: store.epoch, revision: 0, entries: [] } },
      { type: "experimental.space.updated", data: { space: "app", key: "k", value: { n: 1 }, revision: 1 } },
      { type: "agent.updated", data: {} },
    ])
    expect(yield* take(other, 2)).toEqual([
      { type: "experimental.space.snapshot", data: { space: "other", epoch: store.epoch, revision: 0, entries: [] } },
      { type: "agent.updated", data: {} },
    ])
    expect(yield* take(plain, 1)).toEqual([{ type: "agent.updated", data: {} }])
    // Changes never pass through the bus, which is where plugins listen.
    expect(seen).toEqual([Agent.Event.Updated.type])
  }),
)

it.live("sends a large space in batches, each covering every change up to its revision", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store, feed } = yield* boot()
    const count = SpaceStore.BatchSize * 2 + 1
    yield* Effect.forEach(
      Array.from({ length: count }, (_, index) => index),
      (index) => store.set("big", `k${index}`, index),
      {
        discard: true,
      },
    )
    yield* store.remove("big", "k0")

    const live = yield* feed.subscribe({ "experimental.space": ["big"] })
    yield* store.set("big", "after", true)
    const [first, second, third, change] = yield* take(live, 4)
    expect(
      [first, second, third].map((batch) => [batch?.type, batch?.data.revision, batch?.data.entries.length]),
    ).toEqual([
      ["experimental.space.snapshot", SpaceStore.BatchSize + 1, SpaceStore.BatchSize],
      ["experimental.space.delta", SpaceStore.BatchSize * 2 + 1, SpaceStore.BatchSize],
      // The last batch reports the head the snapshot started from.
      ["experimental.space.delta", count + 1, 0],
    ])
    // A snapshot reads no tombstones, so a fresh client hears of no deletion, k0's included.
    expect([second?.data.deleted, third?.data.deleted]).toEqual([[], []])
    expect(change).toEqual({
      type: "experimental.space.updated",
      data: { space: "big", key: "after", value: true, revision: count + 2 },
    })
  }),
)

it.live("a cursor below the space's floor gets a snapshot and one at or above it a delta, after a restart too", () =>
  Effect.gen(function* () {
    const { boot, db } = yield* setup
    const before = yield* boot()
    yield* Effect.forEach(["a", "b", "c", "d"], (key) => before.store.set("app", key, key), { discard: true })
    // With a limit of 2, the third delete purges the first tombstone, at revision 5.
    yield* Effect.forEach(["a", "b", "c"], (key) => Space.remove(db, "app", key, 2), { discard: true })

    const { store } = yield* boot()
    const epoch = store.epoch
    expect(
      yield* collect(store, [
        { space: "app", epoch, since: 4 },
        { space: "app", epoch, since: 5 },
        { space: "app", epoch, since: 6 },
      ]),
    ).toEqual([
      // A delta from 4 could no longer report that a was deleted.
      {
        type: "experimental.space.snapshot",
        data: { space: "app", epoch, revision: 7, entries: [{ key: "d", value: "d" }] },
      },
      {
        type: "experimental.space.delta",
        data: { space: "app", epoch, revision: 7, entries: [], deleted: ["b", "c"] },
      },
      { type: "experimental.space.delta", data: { space: "app", epoch, revision: 7, entries: [], deleted: ["c"] } },
    ])
  }),
)

it.live("changes made while a snapshot goes out reach the client once, and every batch's revision resumes", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const count = SpaceStore.BatchSize + 10
    let paged = false
    const { store, feed } = yield* boot((store) => ({
      ...store,
      // After the first batch: a delete and a rewrite of keys it held, and of keys the second batch would hold.
      read: (request) =>
        store.read(request).pipe(
          Stream.tap(() =>
            Effect.suspend(() => {
              if (paged) return Effect.void
              paged = true
              return Effect.all([
                store.remove("big", "k0"),
                store.set("big", "k1", "new"),
                store.set("big", `k${SpaceStore.BatchSize + 5}`, "new"),
                store.remove("big", `k${SpaceStore.BatchSize + 6}`),
              ])
            }),
          ),
        ),
    }))
    yield* Effect.forEach(range(count), (index) => store.set("big", `k${index}`, index), { discard: true })

    const live = yield* feed.subscribe({ "experimental.space": ["big"] })
    yield* store.set("big", "after", true)
    const batches = yield* take(live, 4)
    const [first, second, third, change] = batches
    expect(batches.slice(0, 3).map((batch) => [batch.type, batch.data.revision, batch.data.entries.length])).toEqual([
      ["experimental.space.snapshot", SpaceStore.BatchSize, SpaceStore.BatchSize],
      // The rest of the keys up to the snapshot's head, without the two changed meanwhile.
      ["experimental.space.delta", count, 8],
      ["experimental.space.delta", count + 4, 2],
    ])
    expect(second?.data.deleted).toEqual([])
    expect(third?.data).toEqual({
      space: "big",
      epoch: store.epoch,
      revision: count + 4,
      entries: [
        { key: "k1", value: "new" },
        { key: `k${SpaceStore.BatchSize + 5}`, value: "new" },
      ],
      deleted: ["k0", `k${SpaceStore.BatchSize + 6}`],
    })
    // The changes the batches already hold are not sent again.
    expect(change).toEqual({
      type: "experimental.space.updated",
      data: { space: "big", key: "after", value: true, revision: count + 5 },
    })

    const current = (yield* collect(store, [{ space: "big" }])).reduce(apply, new Map())
    expect(current.has("k0")).toBe(false)
    const held = new Map<string, unknown>()
    for (const batch of [first, second, third]) {
      if (!batch) continue
      apply(held, batch)
      const resumed = yield* collect(store, [{ space: "big", epoch: store.epoch, since: batch.data.revision }])
      expect(resumed.map((item) => item.type)).not.toContain("experimental.space.snapshot")
      expect(resumed.reduce(apply, new Map(held))).toEqual(current)
    }
  }),
)

it.live("a delta whose deletions are purged while it goes out continues as a snapshot", () =>
  Effect.gen(function* () {
    const { boot, db } = yield* setup
    const { store } = yield* boot()
    // The client holds k0 to k9, at revision 10.
    yield* Effect.forEach(range(10), (index) => store.set("app", `k${index}`, index), { discard: true })
    const held = new Map(range(10).map((index) => [`k${index}`, index]))
    // Then come a full batch of new keys and, with a limit of 2, two tombstones.
    yield* Effect.forEach(range(SpaceStore.BatchSize), (index) => store.set("app", `u${index}`, index), {
      discard: true,
    })
    yield* Space.remove(db, "app", "k0", 2)
    yield* Space.remove(db, "app", "k1", 2)

    let paged = false
    const batches = yield* store.read({ space: "app", epoch: store.epoch, since: 10 }).pipe(
      // After the first batch, a third delete purges k0's tombstone before the second batch can read it.
      Stream.tap(() =>
        Effect.suspend(() => {
          if (paged) return Effect.void
          paged = true
          return Space.remove(db, "app", "k2", 2)
        }),
      ),
      Stream.runCollect,
      Effect.map((items) => Array.from(items)),
    )
    const head = SpaceStore.BatchSize + 13
    expect(batches.map((batch) => [batch.type, batch.data.revision, batch.data.entries.length])).toEqual([
      ["experimental.space.delta", SpaceStore.BatchSize + 10, SpaceStore.BatchSize],
      // k3 to k9, then the new keys, up to the head when the snapshot started.
      ["experimental.space.snapshot", SpaceStore.BatchSize + 3, SpaceStore.BatchSize],
      ["experimental.space.delta", head, 7],
    ])
    expect(batches.reduce(apply, held)).toEqual((yield* collect(store, [{ space: "app" }])).reduce(apply, new Map()))
    expect(held.has("k0")).toBe(false)
  }),
)
