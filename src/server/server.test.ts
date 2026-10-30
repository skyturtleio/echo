import * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "@effect/vitest"
import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Record from "effect/Record"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpEffect } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- real socket fixture in Vitest's Node runtime, not an engine dependency
import { createServer } from "node:http"
import { Note } from "../../examples/fixture/domain/src/Note/Note.aggregate.ts"
import { DefineTag } from "../../examples/fixture/domain/src/Tag/DefineTag.command.ts"
import { Tag } from "../../examples/fixture/domain/src/Tag/Tag.aggregate.ts"
import { TagHistoryImported } from "../../examples/fixture/domain/src/Tag/TagHistoryImported.event.ts"
import type * as Aggregate from "../domain/Aggregate.ts"
import { Principal, privateScope } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"
import * as Server from "./index.ts"

// oxlint-disable-next-line effecttsgo/process-env -- test discovery happens before the Effect runtime
const url = process.env["ECHO_PG_URL"]
const alice = Principal.make("alice")
const bob = Principal.make("bob")
class UndeclaredCommand extends DefineTag {
  static override readonly reads: readonly [] = []
}
class UndeclaredTag extends Tag {
  static override readonly definition: typeof Tag.definition = {
    ...Tag.definition,
    commands: [UndeclaredCommand, Tag.definition.commands[1]]
  }
}
const request = (id: string, target = id, name = id): Wire.ExecuteRequest => ({
  epoch: null,
  clientId: "device",
  batches: [{ id, commands: [{ aggregate: "Tag", command: { _tag: "DefineTag", target, name } }] }]
})
const cursors = (pull: Wire.PullResponse) =>
  Record.fromEntries(Arr.map(pull.scopes, (s): [string, number] => [s.scope, s.cursor]))
const entries = (pull: Wire.PullResponse) => Arr.flatMap(pull.scopes, (s) => s.entries)
const note = (id: string, tagId: string): Wire.ExecuteRequest => ({
  epoch: null,
  clientId: "device",
  batches: [{ id, commands: [{ aggregate: "Note", command: { _tag: "WriteNote", target: id, tagId, text: "text" } }] }]
})

// Every scenario owns a temporary schema, including its engine singleton and listener connections.
let nextSchema = 0
const scenario = <A, E>(
  body: (server: Server.Server, sql: PgClient.PgClient) => Effect.Effect<A, E, Scope.Scope>,
  maxConnections = 12
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const schema = `echo_test_${process.pid}_${nextSchema++}`
      const layer = PgClient.layer({
        url: Redacted.make(url ?? ""),
        startupParameters: { search_path: schema },
        maxConnections
      })
      return yield* Effect.gen(function* () {
        const sql = yield* PgClient.PgClient
        yield* sql`CREATE SCHEMA ${sql(schema)}`
        yield* Effect.addFinalizer(() => sql`DROP SCHEMA ${sql(schema)} CASCADE`.pipe(Effect.orDie))
        yield* Server.migrate
        return yield* body(yield* Server.make({ aggregates: [Tag, Note] }), sql)
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each scenario is a standalone runtime entry point
      }).pipe(Effect.scoped, Effect.provide(layer))
    })
  )

describe.skipIf(url === undefined)("Postgres server", () => {
  it.effect("INV-SERVER-1 retrying accepted or rejected batches cannot append or leak partial state", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        const input = request("accepted")
        const first = yield* server.execute(alice, input)
        expect(first.receipts[0]?._tag).toBe("Accepted")
        expect(yield* server.execute(alice, input)).toEqual(first)
        const rejected: Wire.ExecuteRequest = {
          epoch: first.epoch,
          clientId: "device",
          batches: [
            {
              id: "rollback",
              commands: [
                { aggregate: "Tag", command: { _tag: "DefineTag", target: "temporary", name: "temporary" } },
                { aggregate: "Tag", command: { _tag: "DefineTag", target: "duplicate", name: "accepted" } }
              ]
            }
          ]
        }
        const refusal = yield* server.execute(alice, rejected)
        expect(refusal.receipts[0]).toMatchObject({ _tag: "Rejected", rejection: { _tag: "DuplicateName" } })
        expect(yield* server.execute(alice, rejected)).toEqual(refusal)
        expect(entries(yield* server.pull(alice, { epoch: first.epoch, cursors: {} }))).toHaveLength(1)
        expect(yield* sql`SELECT * FROM echo_events`).toHaveLength(1)
        const collision = yield* Effect.result(server.execute(alice, request("accepted", "other", "changed")))
        expect(collision._tag).toBe("Failure")
        const other = yield* server.execute(bob, request("accepted", "bob-tag", "accepted"))
        expect(other.receipts[0]).toMatchObject({ _tag: "Accepted", seqs: [{ id: "bob-tag" }] })

        const imported = new TagHistoryImported({
          snapshot: { name: "historical", scope: privateScope(alice), removed: false }
        })
        const payload = yield* Schema.encodeEffect(Schema.toCodecJson(TagHistoryImported))(imported)
        const decoded = yield* Schema.decodeEffect(Schema.toCodecJson(TagHistoryImported))(payload)
        const state = Tag.definition.evolve.TagHistoryImported(Tag.definition.initial, decoded)
        expect(state).toEqual({ name: "historical", scope: "user:alice", removed: false })
        yield* sql`INSERT INTO echo_events VALUES ('Tag','imported',1,${sql.json(payload)},'trusted-import','operator',now())`
        yield* sql`INSERT INTO echo_streams VALUES ('Tag','imported','user:alice',1,${sql.json(state)})`
        const remove: Wire.ExecuteRequest = {
          epoch: first.epoch,
          clientId: "device",
          batches: [
            {
              id: "remove-imported",
              commands: [{ aggregate: "Tag", command: { _tag: "RemoveTag", target: "imported" } }]
            }
          ]
        }
        expect((yield* server.execute(alice, remove)).receipts[0]).toMatchObject({
          _tag: "Accepted",
          seqs: [{ id: "imported", seq: 2 }]
        })
        expect(yield* sql`SELECT state FROM echo_streams WHERE id='imported'`).toEqual([
          { state: { name: "historical", scope: "user:alice", removed: true } }
        ])
        expect(yield* sql`SELECT seq,payload FROM echo_events WHERE id='imported' ORDER BY seq`).toEqual([
          { seq: 1, payload },
          { seq: 2, payload: { _tag: "TagRemoved" } }
        ])

        // An untyped domain returning a registered historical event still cannot bypass command output validation.
        // oxlint-disable-next-line typescript/no-extraneous-class -- preserve the aggregate constructor contract at the erased domain boundary
        const invalid: Aggregate.Any = Object.assign(class {}, {
          kind: Tag.kind,
          tag: Tag.tag,
          definition: { ...Tag.definition, decide: { ...Tag.definition.decide, DefineTag: () => [imported] } }
        })
        const invalidServer = yield* Server.make({ aggregates: [invalid, Note] }).pipe(
          Effect.provideService(PgClient.PgClient, sql)
        )
        expect((yield* Effect.exit(invalidServer.execute(alice, request("illegal-import"))))._tag).toBe("Failure")
        expect(yield* sql`SELECT * FROM echo_events WHERE id='illegal-import'`).toHaveLength(0)
        expect(yield* sql`SELECT * FROM echo_receipts WHERE batch_id='illegal-import'`).toHaveLength(0)
      })
    )
  )

  it.effect("INV-SERVER-2 concurrent unique claims and duplicate deliveries have one authoritative outcome", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        const results = yield* Effect.all(
          [server.execute(alice, request("a", "a", "unique")), server.execute(alice, request("b", "b", "unique"))],
          { concurrency: "unbounded" }
        )
        expect(Arr.filter(results, (r) => r.receipts[0]?._tag === "Accepted")).toHaveLength(1)
        expect(Arr.filter(results, (r) => r.receipts[0]?._tag === "Rejected")).toHaveLength(1)
        const repeated = yield* Effect.all(
          Arr.makeBy(4, () => server.execute(alice, request("same"))),
          { concurrency: "unbounded" }
        )
        expect(Arr.every(repeated, (r) => r.receipts[0]?._tag === "Accepted")).toBe(true)
        const rejected = yield* Effect.all(
          Arr.makeBy(4, () => server.execute(alice, request("same-rejection", "reject", "unique"))),
          { concurrency: "unbounded" }
        )
        expect(Arr.every(rejected, (r) => r.receipts[0]?._tag === "Rejected")).toBe(true)
        expect(yield* sql`SELECT * FROM echo_events`).toHaveLength(2)
      })
    )
  )

  it.effect("INV-SERVER-3 hidden targets cannot be overwritten and reads include catalogs but not other users", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        yield* server.execute(bob, request("private", "hidden", "secret"))
        yield* server.ingestCatalog({
          scope: "catalog:tags",
          release: "r1",
          rows: [
            { aggregate: "Tag", id: "catalog-tag", state: { name: "public", scope: "catalog:tags", removed: false } }
          ]
        })
        const forbidden = yield* server.execute(alice, request("overwrite", "hidden", "overwrite"))
        expect(forbidden.receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "ScopeForbidden", scope: "user:alice" }
        })
        expect((yield* server.execute(alice, request("duplicate", "own", "public"))).receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "DuplicateName", existing: "catalog-tag" }
        })
        expect((yield* server.execute(alice, note("hidden-read", "hidden"))).receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "UnknownTag" }
        })
        expect((yield* server.execute(alice, note("catalog-read", "catalog-tag"))).receipts[0]?._tag).toBe("Accepted")
        expect((yield* server.execute(alice, request("catalog-write", "catalog-tag"))).receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "ScopeForbidden" }
        })
        expect(
          Arr.some(entries(yield* server.pull(alice, { epoch: null, cursors: {} })), (e) => e.id === "hidden")
        ).toBe(false)
        const invalid = yield* Server.make({ aggregates: [UndeclaredTag, Note] }).pipe(
          Effect.provideService(PgClient.PgClient, sql)
        )
        expect(yield* Effect.result(invalid.execute(alice, request("undeclared")))).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "ServerError", message: "Invalid domain decision or undeclared read" }
        })
      })
    )
  )

  it.effect("INV-SERVER-4 reconnect cursors retain unchanged rows and epoch mismatch cannot execute", () =>
    scenario(
      Effect.fnUntraced(function* (server) {
        yield* server.execute(alice, request("first"))
        const before = yield* server.pull(alice, { epoch: null, cursors: {} })
        yield* Effect.all(
          Arr.makeBy(5, (i) => server.execute(alice, request(`next-${i}`))),
          { concurrency: "unbounded" }
        )
        const after = yield* server.pull(alice, { epoch: before.epoch, cursors: cursors(before) })
        expect(entries(after)).toHaveLength(6)
        expect(entries(after)).toContainEqual({
          _tag: "Upsert",
          aggregate: "Tag",
          id: "first",
          seq: 1,
          state: { name: "first", scope: "user:alice", removed: false }
        })
        expect(entries(yield* server.pull(alice, { epoch: after.epoch, cursors: cursors(after) }))).toEqual([])
        const epoch = yield* server.resetEpoch
        const oldWrite = yield* Effect.result(
          server.execute(alice, { ...request("must-not-write"), epoch: before.epoch })
        )
        expect(oldWrite).toMatchObject({ _tag: "Failure", failure: { _tag: "ResyncRequired", epoch } })
        expect((yield* Effect.result(server.pull(alice, { epoch: before.epoch, cursors: {} })))._tag).toBe("Failure")
        expect((yield* Effect.result(server.execute(alice, { ...request("first"), epoch: before.epoch })))._tag).toBe(
          "Failure"
        )
        expect(entries(yield* server.pull(alice, { epoch: null, cursors: {} }))).toHaveLength(6)
      })
    )
  )

  it.effect("INV-SERVER-5 catalog replacement is atomic state-only and unchanged catalogs are not downloaded", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        const catalog = {
          scope: "catalog:tags",
          release: "one",
          rows: [
            { aggregate: "Tag", id: "one", state: { name: "one", scope: "catalog:tags", removed: false } },
            { aggregate: "Tag", id: "two", state: { name: "two", scope: "catalog:tags", removed: false } }
          ]
        }
        yield* server.ingestCatalog(catalog)
        const before = yield* server.pull(alice, { epoch: null, cursors: {} })
        expect(entries(before)).toHaveLength(2)
        yield* server.execute(alice, request("own"))
        const unchanged = yield* server.pull(alice, { epoch: before.epoch, cursors: cursors(before) })
        expect(Option.getOrUndefined(Arr.findFirst(unchanged.scopes, (s) => s.scope === catalog.scope))).toMatchObject({
          reset: false,
          entries: []
        })
        expect(yield* sql`SELECT * FROM echo_events`).toHaveLength(1)
        expect(yield* sql`SELECT * FROM echo_catalog_releases`).toHaveLength(1)
        // A duplicate id fails during insertion, after DELETE; the previous snapshot must survive rollback.
        const duplicate = {
          aggregate: "Tag",
          id: "duplicate",
          state: { name: "duplicate", scope: catalog.scope, removed: false }
        }
        expect(
          (yield* Effect.result(
            server.ingestCatalog({ ...catalog, release: "rollback", rows: [duplicate, duplicate] })
          ))._tag
        ).toBe("Failure")
        expect(
          Option.getOrUndefined(
            Arr.findFirst(
              (yield* server.pull(alice, { epoch: null, cursors: {} })).scopes,
              (s) => s.scope === catalog.scope
            )
          )
        ).toEqual(Option.getOrUndefined(Arr.findFirst(before.scopes, (s) => s.scope === catalog.scope)))
        expect(yield* sql`SELECT * FROM echo_catalog_releases`).toHaveLength(1)
        yield* server.ingestCatalog({ ...catalog, release: "empty", rows: [] })
        const emptied = yield* server.pull(alice, { epoch: before.epoch, cursors: cursors(before) })
        expect(Option.getOrUndefined(Arr.findFirst(emptied.scopes, (s) => s.scope === catalog.scope))).toMatchObject({
          reset: true,
          entries: []
        })
        expect(
          (yield* Effect.result(
            server.ingestCatalog({
              ...catalog,
              release: "bad",
              rows: [{ aggregate: "Tag", id: "bad", state: { name: "bad", scope: "user:bob", removed: false } }]
            })
          ))._tag
        ).toBe("Failure")
        expect(yield* sql`SELECT * FROM echo_catalog_releases`).toHaveLength(2)
      })
    )
  )

  it.effect("INV-SERVER-6 HTTP RPC authenticates outside payload and changes reconnect with a catchup poke", () =>
    scenario(
      Effect.fnUntraced(function* (server) {
        const web = HttpEffect.toWebHandler(
          Server.httpApp(server, (req) =>
            req.headers["authorization"] === "Bearer fixture"
              ? Effect.succeed(alice)
              : Effect.fail(new Wire.ServerError({ message: "Unauthorized" }))
          )
        )
        const context = yield* Effect.context<never>()
        const listener = createServer((incoming, outgoing) => {
          const handle = Effect.gen(function* () {
            const chunks = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Uint8Array))(
              yield* Effect.promise(() => incoming.toArray())
            )
            const headers = new Headers()
            for (const [key, value] of Record.toEntries(incoming.headers))
              if (typeof value === "string") headers.set(key, value)
            // oxlint-disable-next-line effecttsgo/abort-controller-in-effect -- cancellation originates from the Node socket, outside the request Effect
            const controller = new AbortController()
            outgoing.on("close", () => controller.abort())
            const response = yield* Effect.promise(() =>
              web(
                new Request("http://localhost/sync", {
                  method: "POST",
                  headers,
                  body: Buffer.concat(chunks),
                  signal: controller.signal
                })
              )
            )
            outgoing.writeHead(response.status, Record.fromEntries(response.headers))
            if (response.body) {
              const reader = response.body.getReader()
              while (true) {
                const chunk = yield* Effect.promise(() => reader.read())
                if (chunk.done) break
                outgoing.write(chunk.value)
              }
              outgoing.end()
            } else outgoing.end()
          })
          Effect.runForkWith(context)(
            handle.pipe(
              Effect.catchCause(() =>
                Effect.sync(() => {
                  outgoing.statusCode = 500
                  outgoing.end()
                })
              )
            )
          )
        })
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            listener.closeAllConnections()
            listener.close()
          })
        )
        yield* Effect.callback<void>((resume) => {
          listener.listen(0, "127.0.0.1", () => resume(Effect.void))
        })
        const address = listener.address()
        if (address === null || typeof address === "string") return yield* Effect.die("Missing listener address")
        const unauthenticated = RpcClient.layerProtocolHttp({ url: `http://127.0.0.1:${address.port}` }).pipe(
          Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson])
        )
        const denied = yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(Wire.EchoRpcs)
          return yield* Effect.result(client.Execute(request("unauthorized")))
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- unauthenticated RPC test runtime
        }).pipe(Effect.provide(unauthenticated))
        expect(denied._tag).toBe("Failure")
        expect(entries(yield* server.pull(alice, { epoch: null, cursors: {} }))).toHaveLength(0)
        const protocol = RpcClient.layerProtocolHttp({
          url: `http://127.0.0.1:${address.port}`,
          transformClient: (client) =>
            HttpClient.mapRequest(client, HttpClientRequest.setHeader("authorization", "Bearer fixture"))
        }).pipe(Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]))
        yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(Wire.EchoRpcs)
          const queue = yield* Queue.unbounded<Wire.Change>()
          const fiber = yield* client.Changes({}).pipe(
            Stream.take(2),
            Stream.runForEach((change) => Queue.offer(queue, change)),
            Effect.forkScoped
          )
          expect((yield* Queue.take(queue))._tag).toBe("Poke")
          expect((yield* client.Execute(request("notification"))).receipts[0]?._tag).toBe("Accepted")
          expect((yield* Queue.take(queue))._tag).toBe("Poke")
          yield* Fiber.join(fiber)
          expect(entries(yield* client.Pull({ epoch: null, cursors: {} }))).toMatchObject([
            { state: { scope: "user:alice" } }
          ])
          expect(yield* client.Changes({}).pipe(Stream.take(1), Stream.runCollect)).toHaveLength(1)
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- real RPC client runtime entry point
        }).pipe(Effect.provide(protocol))
      }),
      1
    )
  )
})
