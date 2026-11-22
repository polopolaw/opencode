import { Space } from "@opencode/schema/space"
import { ForbiddenError, InvalidRequestError } from "../errors.js"
import { Schema } from "effect"
import { HttpApiEndpoint, HttpApiSchema, OpenApi } from "effect/http-api"

const params = { space: Space.Name, key: Space.Key }
const annotations = (identifier: string, summary: string, description: string) =>
  OpenApi.annotations({
    identifier: `experimental.space.${identifier}`,
    summary,
    description: `${description} Subscribers read values through \`experimental.space\` on /api/event. Experimental: needs the \`spaces\` flag, and may change without compatibility guarantees.`,
    // The routes share the `server.experimental` group with the persistent PTY prototype; tag them on their own.
    transform: (operation) => ({ ...operation, tags: ["experimental"] }),
  })

/** Lives in the `server.experimental` group (see `groups/experimental.ts`). */
export const SpaceEndpoints = [
  HttpApiEndpoint.put("space.set", "/api/experimental/space/:space/:key", {
    params,
    payload: Schema.Struct({ value: Space.Value }),
    error: [InvalidRequestError, ForbiddenError],
    success: HttpApiSchema.NoContent,
  }).annotateMerge(
    annotations(
      "set",
      "Set value",
      "Store a client-defined JSON value of at most 1 MiB, replacing any previous value, and send it to the space's subscribers. The server does not interpret it.",
    ),
  ),
  HttpApiEndpoint.delete("space.delete", "/api/experimental/space/:space/:key", {
    params,
    error: ForbiddenError,
    success: HttpApiSchema.NoContent,
  }).annotateMerge(
    annotations(
      "delete",
      "Delete value",
      "Remove a stored value and tell the space's subscribers. Removing an unset key succeeds.",
    ),
  ),
  HttpApiEndpoint.post("space.reset", "/api/experimental/space/reset", {
    error: ForbiddenError,
    success: HttpApiSchema.NoContent,
  }).annotateMerge(
    annotations(
      "reset",
      "Reset revisions",
      "Replace the epoch, so every client's saved revision becomes unusable and its next subscription starts from a snapshot. Use after restoring the database from a backup.",
    ),
  ),
] as const
