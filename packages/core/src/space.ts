export * as Space from "./space.js"

import { and, asc, count, eq, gt, isNotNull, isNull, lte, max, sql } from "drizzle-orm"
import { Effect } from "effect"
import type { Space as SpaceSchema } from "@opencode/schema/space"
import type { Database } from "./database/database.js"
import { SpaceTable } from "./space/sql.js"

type Db = Database.Interface["db"]

// Client space names need at least one character, so the empty space can hold the server's own rows: the epoch and
// each space's floor.
const Meta = ""
const floorKey = (space: SpaceSchema.Name) => `floor:${space}`

// One past the space's highest revision, read by the statement that writes the row. SQLite runs one write at a time,
// so revisions follow commit order, and purges keep the newest tombstone, so none is reissued after a restart.
const next = (space: SpaceSchema.Name) =>
  sql<SpaceSchema.Revision>`(SELECT coalesce(max(${SpaceTable.revision}), 0) + 1 FROM ${SpaceTable} WHERE ${SpaceTable.space} = ${space})`

/** Stores the value and returns the revision it was given. */
export const set = (db: Db, space: SpaceSchema.Name, key: SpaceSchema.Key, value: SpaceSchema.Value) =>
  db
    .insert(SpaceTable)
    .values({ space, key, value, revision: next(space) })
    .onConflictDoUpdate({
      target: [SpaceTable.space, SpaceTable.key],
      set: { value, revision: sql`excluded.${sql.identifier("revision")}` },
    })
    .returning({ revision: SpaceTable.revision })
    .get()
    .pipe(
      Effect.orDie,
      Effect.flatMap((row) => (row ? Effect.succeed(row.revision) : Effect.die(new Error("Upsert returned no row")))),
    )

/** Tombstones kept per space. A delete past it purges the oldest ones and raises the space's floor to the last purged. */
export const TombstoneLimit = 10_000

/** Turns a stored value into a tombstone; returns its revision, or undefined when no value was stored. */
export const remove = (db: Db, space: SpaceSchema.Name, key: SpaceSchema.Key, limit = TombstoneLimit) =>
  db
    .transaction((tx) =>
      Effect.gen(function* () {
        const row = yield* tx
          .update(SpaceTable)
          .set({ value: null, revision: next(space) })
          .where(and(eq(SpaceTable.space, space), eq(SpaceTable.key, key), isNotNull(SpaceTable.value)))
          .returning({ revision: SpaceTable.revision })
          .get()
        if (!row) return undefined
        const tombstones = and(eq(SpaceTable.space, space), isNull(SpaceTable.value))
        const stored = (yield* tx.select({ count: count() }).from(SpaceTable).where(tombstones).get())?.count ?? 0
        if (stored <= limit) return row.revision
        // The tombstone just written is the newest and stays, so the space's highest revision does not move.
        const purged = yield* tx
          .select({ revision: SpaceTable.revision })
          .from(SpaceTable)
          .where(tombstones)
          .orderBy(asc(SpaceTable.revision))
          .limit(1)
          .offset(stored - limit - 1)
          .get()
        if (!purged) return yield* Effect.die(new Error("Space tombstones changed inside their transaction"))
        yield* tx
          .delete(SpaceTable)
          .where(and(tombstones, lte(SpaceTable.revision, purged.revision)))
          .run()
        yield* tx
          .insert(SpaceTable)
          .values({ space: Meta, key: floorKey(space), value: purged.revision, revision: 0 })
          .onConflictDoUpdate({ target: [SpaceTable.space, SpaceTable.key], set: { value: purged.revision } })
          .run()
        return row.revision
      }),
    )
    .pipe(Effect.orDie)

/** The highest purged revision of the space, 0 before any: a delta needs a cursor at or above it. */
export const floor = (db: Db, space: SpaceSchema.Name) =>
  db
    .select({ value: SpaceTable.value })
    .from(SpaceTable)
    .where(and(eq(SpaceTable.space, Meta), eq(SpaceTable.key, floorKey(space))))
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => (typeof row?.value === "number" ? row.value : 0)),
    )

/** The space's highest revision, 0 before its first write. */
export const head = (db: Db, space: SpaceSchema.Name) =>
  db
    .select({ revision: max(SpaceTable.revision) })
    .from(SpaceTable)
    .where(eq(SpaceTable.space, space))
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => row?.revision ?? 0),
    )

/** Rows, tombstones included, written after `since`, in revision order: one range of the revision index. */
export const changes = (db: Db, space: SpaceSchema.Name, since: SpaceSchema.Revision, limit?: number) =>
  db
    .select({ key: SpaceTable.key, value: SpaceTable.value, revision: SpaceTable.revision })
    .from(SpaceTable)
    .where(and(eq(SpaceTable.space, space), gt(SpaceTable.revision, since)))
    .orderBy(asc(SpaceTable.revision))
    .limit(limit ?? -1)
    .all()
    .pipe(Effect.orDie)

/** Rows holding a value, written after `since` and up to `until`, in revision order: one range of the live index. */
export const live = (
  db: Db,
  space: SpaceSchema.Name,
  since: SpaceSchema.Revision,
  until: SpaceSchema.Revision,
  limit: number,
) =>
  db
    .select({ key: SpaceTable.key, value: SpaceTable.value, revision: SpaceTable.revision })
    .from(SpaceTable)
    .where(
      and(
        eq(SpaceTable.space, space),
        isNotNull(SpaceTable.value),
        gt(SpaceTable.revision, since),
        lte(SpaceTable.revision, until),
      ),
    )
    .orderBy(asc(SpaceTable.revision))
    .limit(limit)
    .all()
    .pipe(Effect.orDie)

/** This database's epoch: created once in the empty space, which no client can name, then kept. */
export const epoch = (db: Db) =>
  db
    .insert(SpaceTable)
    .values({ space: Meta, key: "epoch", value: crypto.randomUUID(), revision: 0 })
    .onConflictDoNothing()
    .run()
    .pipe(
      Effect.andThen(
        db
          .select({ value: SpaceTable.value })
          .from(SpaceTable)
          .where(and(eq(SpaceTable.space, Meta), eq(SpaceTable.key, "epoch")))
          .get(),
      ),
      Effect.orDie,
      Effect.flatMap((row) =>
        typeof row?.value === "string" ? Effect.succeed(row.value) : Effect.die(new Error("Space epoch is missing")),
      ),
    )

/** Replaces the epoch, so every client's cursor becomes unusable and it reads a fresh snapshot, as after a restore. */
export const resetEpoch = (db: Db) => {
  const value = crypto.randomUUID()
  return db
    .insert(SpaceTable)
    .values({ space: Meta, key: "epoch", value, revision: 0 })
    .onConflictDoUpdate({ target: [SpaceTable.space, SpaceTable.key], set: { value } })
    .run()
    .pipe(Effect.orDie, Effect.as(value))
}
