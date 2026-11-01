import * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "@effect/vitest"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
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
import { MoveTag, RelocateTag } from "../../examples/fixture/domain/src/Tag/MoveTag.command.ts"
import { TagId } from "../../examples/fixture/domain/src/Tag/Tag.id.ts"
import type * as Aggregate from "../domain/Aggregate.ts"
import { Principal, privateScope, Scope as VisibilityScope, Grant } from "../domain/Scope.ts"
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
    commands: [UndeclaredCommand, Tag.definition.commands[1], Tag.definition.commands[2], Tag.definition.commands[3]]
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
  maxConnections = 12,
  actions: ReadonlyArray<Server.Action.Action> = []
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
        return yield* body(yield* Server.make({ aggregates: [Tag, Note], actions }), sql)
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each scenario is a standalone runtime entry point
      }).pipe(Effect.scoped, Effect.provide(layer))
    })
  )

describe.skipIf(url === undefined)("Postgres server", () => {
  it.effect("INV-SERVER-7 reader grants cannot write and moves evict old readers", () =>
    scenario(
      Effect.fnUntraced(function* (server) {
        const shared = VisibilityScope.make("shared:room")
        const id = TagId.make("shared-tag")
        yield* server.importStreams({
          id: "seed",
          principal: alice,
          streams: [
            Server.importedStream(Tag, id, [
              new TagHistoryImported({ snapshot: { name: "shared", scope: shared, removed: false } })
            ])
          ],
          grants: [
            { scope: shared, principal: alice, role: "writer" },
            { scope: shared, principal: bob, role: "reader" }
          ]
        })
        const before = yield* server.pull(bob, { epoch: null, cursors: {} })
        expect(entries(before)).toHaveLength(1)
        expect((yield* server.execute(bob, note("read", id))).receipts[0]?._tag).toBe("Accepted")
        const move = {
          epoch: null,
          clientId: "device",
          batches: [
            {
              id: "move",
              commands: [{ aggregate: "Tag", command: new MoveTag({ target: id, scope: privateScope(alice) }) }]
            }
          ]
        }
        expect((yield* server.execute(bob, move)).receipts[0]?._tag).toBe("Rejected")
        expect((yield* server.execute(alice, move)).receipts[0]?._tag).toBe("Accepted")
        const after = yield* server.pull(bob, { epoch: null, cursors: cursors(before) })
        expect(Arr.findFirst(after.scopes, (scope) => scope.scope === shared)).toMatchObject({
          value: { reset: true, entries: [] }
        })
        expect(yield* server.get(bob, Tag, id)).toBeNull()
        expect(yield* server.get(alice, Tag, id)).toMatchObject({ scope: privateScope(alice) })
      })
    )
  )

  it.effect("INV-SERVER-8 rejected membership actions cannot leak grants or privileged moves", () => {
    const shared = VisibilityScope.make("shared:atomic")
    const id = TagId.make("atomic")
    const action = Server.Action.make({
      name: "transition",
      input: Schema.Struct({ fail: Schema.Boolean }),
      reads: [Tag],
      decide: ({ fail }, ctx) => {
        if (ctx.principal !== alice || ctx.read.get(Tag, id) === null) return { rejection: { _tag: "Denied" } }
        return {
          operations: [
            Server.Action.grant({ scope: shared, principal: bob, role: "reader" }),
            Server.Action.command(Tag, new RelocateTag({ target: id, scope: shared })),
            ...(fail
              ? [Server.Action.command(Tag, new RelocateTag({ target: TagId.make("absent"), scope: shared }))]
              : []),
            Server.Action.revoke(shared, alice)
          ]
        }
      }
    })
    return scenario(
      Effect.fnUntraced(function* (server, sql) {
        yield* server.execute(alice, request(id))
        const raw = {
          epoch: null,
          clientId: "device",
          batches: [
            { id: "raw", commands: [{ aggregate: "Tag", command: new RelocateTag({ target: id, scope: shared }) }] }
          ]
        }
        expect((yield* server.execute(alice, raw)).receipts[0]?._tag).toBe("Rejected")
        const rejected = {
          epoch: null,
          clientId: "device",
          batches: [{ id: "bad", commands: [], action: { name: "transition", input: { fail: true } } }]
        }
        expect((yield* server.execute(alice, rejected)).receipts[0]?._tag).toBe("Rejected")
        expect(yield* sql`SELECT * FROM echo_grants`).toHaveLength(0)
        expect(yield* server.get(alice, Tag, id)).toMatchObject({ scope: privateScope(alice) })
        const accepted = {
          ...rejected,
          batches: [{ id: "good", commands: [], action: { name: "transition", input: { fail: false } } }]
        }
        const result = yield* server.execute(alice, accepted)
        expect(result.receipts[0]?._tag).toBe("Accepted")
        expect(yield* server.execute(alice, accepted)).toEqual(result)
        expect(yield* server.get(alice, Tag, id)).toBeNull()
        expect(yield* server.get(bob, Tag, id)).toMatchObject({ scope: shared })
        expect(yield* sql`SELECT * FROM echo_events WHERE id=${id}`).toHaveLength(2)
        const pulled = yield* server.pull(alice, { epoch: null, cursors: {} })
        const receipt = result.receipts[0]
        expect(receipt?._tag === "Accepted" && pulled.revision >= receipt.revision).toBe(true)
      }),
      12,
      [action]
    )
  })

  it.effect("INV-SERVER-9 imports replay truthful events and roll back with the enclosing migration", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        const stream = Server.importedStream(Tag, TagId.make("imported"), [
          new TagHistoryImported({ snapshot: { name: "historical", scope: privateScope(alice), removed: false } })
        ])
        const input = { id: "history", principal: alice, streams: [stream] }
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql`SELECT 1`
              yield* server.importStreams(input)
              return yield* Effect.fail("rollback")
            })
          )
          .pipe(Effect.flip)
        expect(yield* server.get(alice, Tag, TagId.make("imported"))).toBeNull()
        yield* server.importStreams(input)
        yield* server.importStreams(input)
        expect(yield* sql`SELECT payload,principal FROM echo_events`).toMatchObject([
          { principal: "alice", payload: { _tag: "TagHistoryImported", snapshot: { name: "historical" } } }
        ])
        expect((yield* server.importStreams({ ...input, id: "conflict" }).pipe(Effect.flip))._tag).toBe("ServerError")
        expect(yield* server.get(alice, Tag, TagId.make("imported"))).toMatchObject({ name: "historical" })
      })
    )
  )

  it.effect("INV-PRESENCE-1 forged connections and revoked rooms cannot disclose presence", () => {
    const room = VisibilityScope.make("shared:presence")
    const revoke = Server.Action.make({
      name: "revoke",
      input: Grant,
      reads: [],
      decide: (grant) => ({ operations: [Server.Action.revoke(grant.scope, grant.principal)] })
    })
    return scenario(
      Effect.fnUntraced(function* (server, sql) {
        yield* server.importStreams({
          id: "members",
          principal: alice,
          streams: [],
          grants: [
            { scope: room, principal: alice, role: "owner" },
            { scope: room, principal: bob, role: "reader" }
          ]
        })
        const queue = yield* Queue.unbounded<Wire.Change>()
        const fiber = yield* server.changes(bob).pipe(
          Stream.runForEach((frame) => Queue.offer(queue, frame)),
          Effect.forkScoped
        )
        const initial = yield* Queue.take(queue)
        const connectionId = initial.connectionId ?? "missing"
        expect(
          (yield* server.presence(alice, { connectionId, room, clientId: "tab", state: {} }).pipe(Effect.flip))._tag
        ).toBe("ServerError")
        yield* server.presence(bob, { connectionId, room, clientId: "tab", state: { active: true } })
        expect((yield* Queue.take(queue)).presence).toMatchObject([{ principal: "bob", state: { active: true } }])
        // Simulate a crashed participant's elapsed lease while its shared connection stays alive.
        yield* sql`UPDATE echo_presence SET expires_at=now()-interval '1 second' WHERE client_id='tab'`
        yield* server.presence(bob, { connectionId, room, clientId: "living", state: {} })
        expect((yield* Queue.take(queue)).presence).toMatchObject([{ clientId: "living" }])
        for (let i = 1; i < 64; i++) {
          yield* server.presence(bob, { connectionId, room, clientId: `living-${i}`, state: {} })
          yield* Queue.take(queue)
        }
        expect(
          (yield* server.presence(bob, { connectionId, room, clientId: "overflow", state: {} }).pipe(Effect.flip))._tag
        ).toBe("ServerError")
        yield* server.presence(bob, { connectionId, room, clientId: "living", state: { updated: true } })
        expect((yield* Queue.take(queue)).presence).toContainEqual({
          room,
          principal: bob,
          clientId: "living",
          state: { updated: true }
        })
        yield* server.execute(alice, {
          epoch: null,
          clientId: "device",
          batches: [
            {
              id: "revoke",
              commands: [],
              action: { name: "revoke", input: { scope: room, principal: bob, role: "reader" } }
            }
          ]
        })
        expect((yield* Queue.take(queue)).presence).toEqual([])
        expect(
          (yield* server.presence(bob, { connectionId, room, clientId: "tab", state: {} }).pipe(Effect.flip))._tag
        ).toBe("ServerError")
        yield* Fiber.interrupt(fiber)
      }),
      12,
      [revoke]
    )
  })

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
        const rejections: Array<Server.RejectedOutcome> = []
        const observed = yield* Server.make({
          aggregates: [Tag, Note],
          onRejected: (outcome) =>
            Effect.sync(() => {
              rejections.push(outcome)
              throw new Error("broken diagnostic sink")
            })
        }).pipe(Effect.provideService(PgClient.PgClient, sql))
        const refusal = yield* observed.execute(alice, rejected)
        expect(refusal.receipts[0]).toMatchObject({ _tag: "Rejected", rejection: { _tag: "DuplicateName" } })
        expect(yield* observed.execute(alice, rejected)).toEqual(refusal)
        expect(rejections).toEqual([{ operation: "Batch", rejectionTag: "DuplicateName" }])
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

        const batch = (id: string, commands: Wire.ExecuteRequest["batches"][number]["commands"]) =>
          server.execute(alice, { epoch: first.epoch, clientId: "device", batches: [{ id, commands }] })
        const define = { aggregate: "Tag", command: { _tag: "DefineTag", target: "in-batch", name: "in-batch" } }
        const write = {
          aggregate: "Note",
          command: { _tag: "WriteNote", target: "in-batch-note", tagId: "in-batch", text: "in-batch" }
        }
        const removeInBatch = { aggregate: "Tag", command: { _tag: "RemoveTag", target: "in-batch" } }
        expect((yield* batch("index-overlay", [define, write, removeInBatch])).receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "TagInUse", notes: 1 }
        })
        expect(yield* sql`SELECT * FROM echo_streams WHERE id IN ('in-batch','in-batch-note')`).toHaveLength(0)
        expect((yield* batch("id-overlay", [define, write])).receipts[0]).toMatchObject({
          _tag: "Accepted",
          seqs: [
            { id: "in-batch", seq: 1 },
            { id: "in-batch-note", seq: 1 }
          ]
        })
        const ephemeral = {
          aggregate: "Tag",
          command: { _tag: "DefineTag", target: "ephemeral", name: "ephemeral" }
        }
        expect(
          (yield* batch("target-overlay", [
            ephemeral,
            {
              aggregate: "Tag",
              command: { _tag: "RemoveTag", target: "ephemeral" }
            }
          ])).receipts[0]
        ).toMatchObject({ _tag: "Accepted", seqs: [{ id: "ephemeral", seq: 2 }] })
        expect(yield* sql`SELECT state->'removed' AS removed FROM echo_streams WHERE id='ephemeral'`).toEqual([
          { removed: true }
        ])
        yield* server.execute(alice, request("persisted-overlay"))
        const removePersisted = {
          aggregate: "Tag",
          command: { _tag: "RemoveTag", target: "persisted-overlay" }
        }
        expect(
          (yield* batch("persisted-target-overlay", [removePersisted, removePersisted])).receipts[0]
        ).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "NoSuchTag" }
        })
        expect(
          yield* sql`SELECT seq,state->'removed' AS removed FROM echo_streams WHERE id='persisted-overlay'`
        ).toEqual([{ seq: 1, removed: false }])
        expect(
          (yield* batch("rejection-precedence", [
            define,
            {
              aggregate: "Tag",
              command: { _tag: "UnknownCommand", target: "unreached" }
            }
          ])).receipts[0]
        ).toMatchObject({ _tag: "Rejected", rejection: { _tag: "TagAlreadyDefined" } })
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
        const inBatch = yield* server.execute(alice, {
          epoch: null,
          clientId: "device",
          batches: [
            {
              id: "duplicate-in-batch",
              commands: [
                { aggregate: "Tag", command: { _tag: "DefineTag", target: "first", name: "same-in-batch" } },
                { aggregate: "Tag", command: { _tag: "DefineTag", target: "second", name: "same-in-batch" } }
              ]
            }
          ]
        })
        expect(inBatch.receipts[0]).toMatchObject({ _tag: "Rejected", rejection: { _tag: "DuplicateName" } })
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
        const diagnostics: string[] = []
        const invalid = yield* Server.make({
          aggregates: [UndeclaredTag, Note],
          onFailure: (operation, cause) =>
            Effect.sync(() => {
              diagnostics.push(`${operation}: ${Cause.pretty(cause)}`)
              throw new Error("broken diagnostic sink")
            })
        }).pipe(Effect.provideService(PgClient.PgClient, sql))
        expect(yield* Effect.result(invalid.execute(alice, request("undeclared")))).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "ServerError", message: "Echo storage operation failed" }
        })
        expect(diagnostics).toHaveLength(1)
        expect(diagnostics[0]).toContain("undeclared read")

        // Removing a tag reads Notes, not other Tags. Unrelated corrupt state must not be decoded.
        yield* server.execute(alice, request("isolated-target"))
        yield* sql`INSERT INTO echo_streams VALUES ('Tag','unrelated','catalog:tags',1,'{}'::jsonb)`
        const removeIsolated: Wire.ExecuteRequest = {
          epoch: null,
          clientId: "device",
          batches: [
            {
              id: "remove-isolated",
              commands: [{ aggregate: "Tag", command: { _tag: "RemoveTag", target: "isolated-target" } }]
            }
          ]
        }
        expect((yield* server.execute(alice, removeIsolated)).receipts[0]).toMatchObject({
          _tag: "Accepted",
          seqs: [{ id: "isolated-target", seq: 2 }]
        })
        expect(yield* sql`SELECT state FROM echo_streams WHERE id='unrelated'`).toEqual([{ state: {} }])
        // The same malformed shape must still fail when it belongs to a declared read aggregate.
        yield* sql`INSERT INTO echo_streams VALUES ('Note','selected','catalog:notes',1,'{}'::jsonb)`
        expect(
          (yield* Effect.result(
            server.execute(alice, {
              ...removeIsolated,
              batches: [
                {
                  id: "selected-invalid",
                  commands: [{ aggregate: "Tag", command: { _tag: "RemoveTag", target: "absent" } }]
                }
              ]
            })
          ))._tag
        ).toBe("Failure")
        expect(yield* sql`SELECT * FROM echo_receipts WHERE batch_id='selected-invalid'`).toHaveLength(0)
        expect(
          (yield* server.execute(alice, {
            epoch: null,
            clientId: "device",
            batches: [{ id: "empty", commands: [] }]
          })).receipts[0]
        ).toMatchObject({ _tag: "Accepted", seqs: [] })
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
