import path from "node:path"
import { expect } from "bun:test"
import { OpenCode } from "@opencode/client"
import { Effect } from "effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const authorization = `Basic ${btoa("opencode:secret")}`

const client = (handler: (request: Request) => Promise<Response>, headers?: Record<string, string>) =>
  OpenCode.make({
    baseUrl: "http://opencode.local",
    headers,
    fetch: Object.assign((input: string | URL | Request, init?: RequestInit) => handler(new Request(input, init)), {
      preconnect: fetch.preconnect,
    }),
  })

// Reads the wire stream directly: the SDK's shared event subscription has no `experimental.space` option.
const subscribe = async (
  handler: (request: Request) => Promise<Response>,
  query: string,
  headers: Record<string, string> = { authorization },
) => {
  const response = await handler(new Request(`http://opencode.local/api/event?${query}`, { headers }))
  const reader = response.body?.pipeThrough(new TextDecoderStream()).getReader()
  const frames: string[] = []
  let buffer = ""
  return {
    status: response.status,
    next: async (): Promise<{ readonly type: string; readonly data: unknown }> => {
      while (frames.length === 0) {
        const chunk = await reader?.read()
        if (!chunk || chunk.done) throw new Error("event stream closed")
        const parts = (buffer + chunk.value).split("\n\n")
        buffer = parts.pop() ?? ""
        frames.push(...parts.filter((part) => part.startsWith("data: ")))
      }
      const event = JSON.parse(frames.shift()?.slice("data: ".length) ?? "")
      return { type: event.type, data: event.data }
    },
    close: () => reader?.cancel(),
  }
}

it.live("syncs requested spaces to another client, catching up from revisions across reconnects and a restart", () =>
  Effect.gen(function* () {
    const dir = (yield* tmpdirScoped("opencode-space-api-")).path
    const boot = ServerFetch.make({
      app: { version: "test" },
      password: "secret",
      database: { path: path.join(dir, "opencode.db") },
      config: {
        directory: dir,
        project: false,
        claude: false,
        content: JSON.stringify({ experimental: { spaces: true } }),
      },
      models: { fetch: false },
      fs: { filewatcher: false },
    })

    // Issued by the database, so it survives the restart below.
    let epoch = ""
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handler = yield* boot
        const writer = client(handler, { authorization })
        yield* Effect.promise(async () => {
          await writer.experimental.space.set({ space: "app", key: "draft/1", value: { text: "hi", n: [1, null] } })
          await writer.experimental.space.set({ space: "other", key: "draft/1", value: "elsewhere" })

          const reader = await subscribe(
            handler,
            "types=session.created&experimental.space=app&experimental.space=empty",
          )
          const plain = await subscribe(handler, "")
          expect(await reader.next()).toEqual({ type: "server.connected", data: {} })
          const first = await reader.next()
          epoch = (first.data as { epoch: string }).epoch
          expect(first).toEqual({
            type: "experimental.space.snapshot",
            data: {
              space: "app",
              epoch,
              revision: 1,
              entries: [{ key: "draft/1", value: { text: "hi", n: [1, null] } }],
            },
          })
          expect(await reader.next()).toEqual({
            type: "experimental.space.snapshot",
            data: { space: "empty", epoch, revision: 0, entries: [] },
          })
          expect(await plain.next()).toEqual({ type: "server.connected", data: {} })

          await writer.experimental.space.set({ space: "other", key: "draft/1", value: "unseen" })
          await writer.experimental.space.set({ space: "app", key: "draft/2", value: 2 })
          await writer.experimental.space.delete({ space: "app", key: "draft/1" })
          await writer.session.create({ location: { directory: dir } })
          expect(await reader.next()).toEqual({
            type: "experimental.space.updated",
            data: { space: "app", key: "draft/2", value: 2, revision: 2 },
          })
          expect(await reader.next()).toEqual({
            type: "experimental.space.deleted",
            data: { space: "app", key: "draft/1", revision: 3 },
          })
          // `types` still selects the other events, and a plain subscriber sees no space events at all.
          expect((await reader.next()).type).toBe("session.created")
          expect((await plain.next()).type).not.toStartWith("experimental.space.")
          await reader.close()
          await plain.close()

          // Offline: repeated updates, a key created and deleted, and deletes of already deleted keys.
          await writer.experimental.space.set({ space: "app", key: "draft/2", value: 3 })
          await writer.experimental.space.set({ space: "app", key: "draft/2", value: 4 })
          await writer.experimental.space.set({ space: "app", key: "draft/3", value: "x" })
          await writer.experimental.space.delete({ space: "app", key: "draft/3" })
          await writer.experimental.space.delete({ space: "app", key: "draft/3" })
          await writer.experimental.space.delete({ space: "app", key: "draft/1" })

          // One stream mixes a delta, an unusable cursor, an empty delta, a revision this server never issued and
          // another database's epoch.
          const reconnected = await subscribe(
            handler,
            `experimental.space=app:${epoch}.3&experimental.space=other:x&experimental.space=empty:${epoch}.0` +
              `&experimental.space=ahead:${epoch}.99&experimental.space=elsewhere:another-epoch.0`,
          )
          await reconnected.next()
          expect(await reconnected.next()).toEqual({
            type: "experimental.space.delta",
            data: { space: "app", epoch, revision: 7, entries: [{ key: "draft/2", value: 4 }], deleted: ["draft/3"] },
          })
          expect(await reconnected.next()).toEqual({
            type: "experimental.space.snapshot",
            data: { space: "other", epoch, revision: 2, entries: [{ key: "draft/1", value: "unseen" }] },
          })
          expect(await reconnected.next()).toEqual({
            type: "experimental.space.delta",
            data: { space: "empty", epoch, revision: 0, entries: [], deleted: [] },
          })
          expect(await reconnected.next()).toEqual({
            type: "experimental.space.snapshot",
            data: { space: "ahead", epoch, revision: 0, entries: [] },
          })
          expect(await reconnected.next()).toEqual({
            type: "experimental.space.snapshot",
            data: { space: "elsewhere", epoch, revision: 0, entries: [] },
          })
          await writer.experimental.space.set({ space: "app", key: "draft/4", value: true })
          expect(await reconnected.next()).toEqual({
            type: "experimental.space.updated",
            data: { space: "app", key: "draft/4", value: true, revision: 8 },
          })
          await reconnected.close()

          expect((await subscribe(handler, "experimental.space=app", {})).status).toBe(401)
          expect((await subscribe(handler, "experimental.space=a/b:1")).status).toBe(400)
          await expect(
            client(handler).experimental.space.set({ space: "app", key: "x", value: 1 }),
          ).rejects.toMatchObject({
            _tag: "UnauthorizedError",
          })
          const empty = await handler(
            new Request("http://opencode.local/api/experimental/space/app/c", {
              method: "PUT",
              headers: { authorization, "content-type": "application/json" },
              body: JSON.stringify({ value: null }),
            }),
          )
          expect(empty.status).toBe(400)
          const large = await handler(
            new Request("http://opencode.local/api/experimental/space/app/c", {
              method: "PUT",
              headers: { authorization, "content-type": "application/json" },
              body: JSON.stringify({ value: "x".repeat(1024 * 1024) }),
            }),
          )
          expect(large.status).toBe(400)
        })
      }),
    )

    const handler = yield* boot
    yield* Effect.promise(async () => {
      const reader = await subscribe(
        handler,
        `types=session.created&experimental.space=app:${epoch}.7&experimental.space=other:${epoch}.2`,
      )
      await reader.next()
      expect(await reader.next()).toEqual({
        type: "experimental.space.delta",
        data: { space: "app", epoch, revision: 8, entries: [{ key: "draft/4", value: true }], deleted: [] },
      })
      expect(await reader.next()).toEqual({
        type: "experimental.space.delta",
        data: { space: "other", epoch, revision: 2, entries: [], deleted: [] },
      })
      await client(handler, { authorization }).experimental.space.delete({ space: "app", key: "draft/4" })
      expect(await reader.next()).toEqual({
        type: "experimental.space.deleted",
        data: { space: "app", key: "draft/4", revision: 9 },
      })
      await reader.close()

      // A reset, as after a restore, makes every saved cursor unusable; only an authenticated client may do it.
      await expect(client(handler).experimental.space.reset()).rejects.toMatchObject({ _tag: "UnauthorizedError" })
      await client(handler, { authorization }).experimental.space.reset()
      const reset = await subscribe(handler, `experimental.space=other:${epoch}.2`)
      await reset.next()
      const snapshot = await reset.next()
      expect(snapshot.type).toBe("experimental.space.snapshot")
      expect((snapshot.data as { epoch: string }).epoch).not.toBe(epoch)
      await reset.close()
    })
  }).pipe(Effect.scoped),
)

it.live("without the spaces flag, writes are forbidden and the stream ignores spaces", () =>
  Effect.gen(function* () {
    const dir = (yield* tmpdirScoped("opencode-space-off-")).path
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      password: "secret",
      database: { path: path.join(dir, "opencode.db") },
      config: {
        directory: dir,
        project: false,
        claude: false,
        content: JSON.stringify({ experimental: { spaces: false } }),
      },
      models: { fetch: false },
      fs: { filewatcher: false },
    })
    yield* Effect.promise(async () => {
      const writer = client(handler, { authorization })
      await expect(writer.experimental.space.set({ space: "app", key: "k", value: 1 })).rejects.toMatchObject({
        _tag: "ForbiddenError",
      })
      await expect(writer.experimental.space.reset()).rejects.toMatchObject({ _tag: "ForbiddenError" })
      const reader = await subscribe(handler, "types=session.created&experimental.space=app")
      expect(await reader.next()).toEqual({ type: "server.connected", data: {} })
      await writer.session.create({ location: { directory: dir } })
      expect((await reader.next()).type).toBe("session.created")
      await reader.close()
    })
  }).pipe(Effect.scoped),
)
