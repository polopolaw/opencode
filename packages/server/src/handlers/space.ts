import { ForbiddenError, InvalidRequestError } from "@opencode/protocol/errors"
import { Space } from "@opencode/schema/space"
import { Effect } from "effect"
import { HttpApiSchema } from "effect/http-api"
import { SpaceStore } from "../space-store"

/** `space.*` in the `server.experimental` group. */
export const makeSpace = Effect.gen(function* () {
  const store = yield* SpaceStore.Service
  const enabled = yield* SpaceStore.enabled
  const requireEnabled = Effect.gen(function* () {
    if (yield* enabled) return
    return yield* new ForbiddenError({ message: "Spaces are disabled; enable experimental.spaces" })
  })

  return {
    set: Effect.fn("SpaceHandler.set")(function* (space: Space.Name, key: Space.Key, value: Space.Value) {
      yield* requireEnabled
      if (new TextEncoder().encode(JSON.stringify(value)).length > Space.MaxValueBytes)
        return yield* new InvalidRequestError({ message: "Value exceeds 1 MiB of JSON", field: "value" })
      yield* store.set(space, key, value)
      return HttpApiSchema.NoContent.make()
    }),
    reset: Effect.fn("SpaceHandler.reset")(function* () {
      yield* requireEnabled
      yield* store.resetEpoch
      return HttpApiSchema.NoContent.make()
    }),
    remove: Effect.fn("SpaceHandler.remove")(function* (space: Space.Name, key: Space.Key) {
      yield* requireEnabled
      yield* store.remove(space, key)
      return HttpApiSchema.NoContent.make()
    }),
  }
})
