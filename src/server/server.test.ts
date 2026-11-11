import * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "@effect/vitest"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Record from "effect/Record"
import * as Redacted from "effect/Redacted"
import * as References from "effect/References"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Str from "effect/String"
import * as Tracer from "effect/Tracer"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpEffect } from "effect/http"
import { Reactivity } from "effect/reactivity"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { SqlClient } from "effect/sql"
import { TestClock } from "effect/testing"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- real socket fixture in Vitest's Node runtime, not an engine dependency
import { createServer } from "node:http"
import { Note } from "../../examples/fixture/domain/src/Note/Note.aggregate.ts"
import { NoteId } from "../../examples/fixture/domain/src/Note/Note.id.ts"
import { WriteNote } from "../../examples/fixture/domain/src/Note/WriteNote.command.ts"
import { DefineTag } from "../../examples/fixture/domain/src/Tag/DefineTag.command.ts"
import { Tag } from "../../examples/fixture/domain/src/Tag/Tag.aggregate.ts"
import { TagHistoryImported } from "../../examples/fixture/domain/src/Tag/TagHistoryImported.event.ts"
import { MoveTag, RelocateTag } from "../../examples/fixture/domain/src/Tag/MoveTag.command.ts"
import { TagId } from "../../examples/fixture/domain/src/Tag/Tag.id.ts"
import {
  LegacyTally,
  CurrentTally,
  ProjectedTally,
  OldTallyAdded,
  TallyAdded
} from "../../examples/fixture/domain/src/Evolution.ts"
import type * as Aggregate from "../domain/Aggregate.ts"
import { capabilities } from "../domain/index.ts"
import { Principal, privateScope, Scope as VisibilityScope, Grant } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"
import * as Server from "./index.ts"

// oxlint-disable-next-line effecttsgo/process-env -- test discovery happens before the Effect runtime
const url = process.env["ECHOSYNC_PG_URL"]
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
class TrustedWriteNote extends WriteNote {
  static override readonly authority = "server" as const
}
class TrustedNote extends Note {
  static override readonly definition: typeof Note.definition = {
    ...Note.definition,
    commands: [TrustedWriteNote]
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

const transfer = (batchId: string, target: string, scope: VisibilityScope): Wire.ExecuteRequest => ({
  epoch: null,
  clientId: "device",
  batches: [{ id: batchId, commands: [{ aggregate: "Tag", command: { _tag: "MoveTag", target, scope } }] }]
})

const tallyBatch = (id: string, command: unknown): Wire.ExecuteRequest => ({
  epoch: null,
  clientId: "old-device",
  batches: [{ id, commands: [{ aggregate: "Tally", command }] }]
})

// Every scenario owns a temporary schema, including its engine singleton and listener connections.
let nextSchema = 0
const scenario = <A, E>(
  body: (server: Server.Server, sql: PgClient.PgClient) => Effect.Effect<A, E, Scope.Scope | PgClient.PgClient>,
  maxConnections = 12,
  actions: ReadonlyArray<Server.Action.Action> = []
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const schema = `echosync_test_${process.pid}_${nextSchema++}`
      const layer = PgClient.layer({
        url: Redacted.make(url ?? ""),
        startupParameters: { search_path: schema, application_name: schema },
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
  it.effect("INV-SERVER-12 operation logs cannot leak payloads or confuse page work with Changes lifetime", () => {
    const logs: Array<{
      readonly message: unknown
      readonly cause: string | undefined
      readonly annotations: Record<string, unknown>
    }> = []
    const logger = Logger.map(Logger.formatStructured, (entry) => {
      logs.push(entry)
    })
    const spans: Array<Tracer.Span> = []
    let tracerFailure: "none" | "create" | "end" = "none"
    const tracer = Tracer.make({
      span(options) {
        const operation = Str.startsWith("EchoSync.operation.")(options.name)
        if (operation && tracerFailure === "create") throw new Error("private-tracer-error")
        const span = Tracer.nativeTracer.span(options)
        if (operation) {
          spans.push(span)
          if (tracerFailure === "end")
            span.end = () => {
              throw new Error("private-tracer-error")
            }
        }
        return span
      }
    })
    return scenario(
      Effect.fnUntraced(function* (server) {
        yield* server.importStreams({
          id: "private-import",
          principal: alice,
          streams: Arr.makeBy(2049, (n) =>
            Server.importedStream(Tag, TagId.make(`private-${n}`), [
              new TagHistoryImported({
                snapshot: { name: "private-payload", scope: privateScope(alice), removed: false }
              })
            ])
          )
        })
        const first = yield* server.pull(alice, { epoch: null, cursors: {}, incremental: true })
        if (first.page?.next === null || first.page?.next === undefined)
          return yield* Effect.die("missing continuation")
        yield* server.pull(alice, {
          epoch: first.epoch,
          cursors: {},
          incremental: true,
          continuation: { id: first.page.id, offset: first.page.next }
        })
        yield* server.pull(alice, { epoch: first.epoch, cursors: cursors(first), incremental: true })
        yield* Effect.exit(server.pull(alice, { epoch: "private-wrong-epoch", cursors: {} }))
        yield* server.execute(alice, request("private-command"))
        yield* server.changes(alice).pipe(Stream.take(1), Stream.runDrain)
        const operations = Arr.filter(logs, (entry) => entry.message === "echosync.operation")
        expect(operations).toHaveLength(12)
        const completed = Arr.map(
          Arr.filter(operations, (entry) => entry.annotations["outcome"] !== "started"),
          (entry) => entry.annotations
        )
        expect(completed).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              operation: "Pull",
              page: "first",
              outcome: "succeeded",
              count: 2048,
              pageCount: 1,
              mode: "bootstrap"
            }),
            expect.objectContaining({
              operation: "Pull",
              page: "continuation",
              outcome: "succeeded",
              count: 1,
              pageCount: 1,
              mode: "bootstrap"
            }),
            expect.objectContaining({ operation: "Pull", count: 0, mode: "incremental" }),
            expect.objectContaining({
              operation: "Pull",
              outcome: "failed",
              failure: "resync",
              count: null,
              mode: null
            }),
            expect.objectContaining({ operation: "Execute", outcome: "succeeded", count: 1 }),
            expect.objectContaining({ operation: "Changes", count: null, pageCount: null, mode: null })
          ])
        )
        for (const entry of operations) {
          expect(entry.cause).toBeUndefined()
          expect(entry.annotations["requestId"]).toBe("test-request")
          expect(entry.annotations["durationMs"]).toBeGreaterThanOrEqual(0)
          expect(
            Arr.filter(operations, (other) => other.annotations["operationId"] === entry.annotations["operationId"])
          ).toHaveLength(2)
          expect(Arr.sort(Record.keys(entry.annotations), Str.Order)).toEqual(
            Arr.sort(
              [
                "operationId",
                "operation",
                "page",
                "outcome",
                "durationMs",
                "failure",
                "count",
                "pageCount",
                "mode",
                "requestId"
              ],
              Str.Order
            )
          )
        }
        expect(yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(operations)).not.toMatch(
          /private-|alice|user:/
        )
        // A broken application logger cannot change the successful page or its row count.
        const broken = Logger.make(() => {
          throw new Error("private-logger-error")
        })
        const unchanged = yield* server
          .pull(alice, { epoch: first.epoch, cursors: cursors(first), incremental: true })
          .pipe(Effect.provideService(Logger.CurrentLoggers, new Set([broken])))
        expect(entries(unchanged)).toHaveLength(1)
        // Interrupt exactly when the started log is emitted, before connection acquisition.
        const interrupting = Logger.make((options) => {
          const entry = Logger.formatStructured.log(options)
          logs.push(entry)
          if (
            entry.message === "echosync.operation" &&
            entry.annotations["operation"] === "Changes" &&
            entry.annotations["outcome"] === "started"
          )
            options.fiber.interruptUnsafe()
        })
        const interrupted = yield* server
          .changes(alice)
          .pipe(
            Stream.runDrain,
            Effect.provideService(Logger.CurrentLoggers, new Set([interrupting])),
            Effect.forkChild
          )
        yield* Fiber.await(interrupted)
        expect(
          Arr.filter(
            logs,
            (entry) => entry.annotations["operation"] === "Changes" && entry.annotations["outcome"] === "interrupted"
          )
        ).toHaveLength(1)
        expect(spans.length).toBe(8)
        for (const span of spans) {
          expect(span.status._tag).toBe("Ended")
          if (span.status._tag === "Ended") expect(span.status.exit).toEqual(Exit.void)
          expect(span.attributes.has("privateAttribute")).toBe(false)
          expect(span.links).toHaveLength(0)
          expect(Arr.sort([...span.attributes.keys()], Str.Order)).toEqual(
            Arr.sort(
              ["operationId", "operation", "page", "outcome", "failure", "durationMs", "count", "pageCount", "mode"],
              Str.Order
            )
          )
        }
        for (const failure of ["create", "end"] as const) {
          tracerFailure = failure
          const before = logs.length
          yield* server.pull(alice, { epoch: first.epoch, cursors: cursors(first), incremental: true })
          expect(Arr.map(logs.slice(before), (entry) => entry.annotations["outcome"])).toEqual(["started", "succeeded"])
        }
      })
    ).pipe(
      Effect.annotateLogs({ requestId: "test-request" }),
      Effect.annotateSpans({ privateAttribute: "private-payload" }),
      Effect.provideService(References.TracerSpanLinks, [
        {
          span: Tracer.externalSpan({ spanId: "private-span", traceId: "private-trace" }),
          attributes: { privateLink: "private-payload" }
        }
      ]),
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.provideService(Logger.CurrentLoggers, new Set([logger]))
    )
  })
  it.effect(
    "INV-SYNC-2 paged catchup cannot skip concurrent edits, deletes or revoked access",
    () => {
      const room = VisibilityScope.make("shared:incremental")
      const membership = Server.Action.make({
        name: "membership",
        input: Schema.Struct({ enabled: Schema.Boolean }),
        reads: [],
        decide: ({ enabled }) => ({
          operations: enabled
            ? [Server.Action.grant({ scope: room, principal: alice, role: "owner" })]
            : [Server.Action.revoke(room, alice)]
        })
      })
      return scenario(
        Effect.fnUntraced(function* (server, sql) {
          const empty = yield* server.pull(alice, { epoch: null, cursors: {}, incremental: true })
          expect(empty.scopes).toEqual([{ scope: "user:alice", cursor: 0, reset: true, entries: [] }])
          yield* server.importStreams({
            id: "many",
            principal: alice,
            streams: Arr.makeBy(2501, (i) =>
              Server.importedStream(Tag, TagId.make(`row-${Str.padStart(4, "0")(String(i))}`), [
                new TagHistoryImported({ snapshot: { name: `name-${i}`, scope: privateScope(alice), removed: false } })
              ])
            ),
            grants: [{ scope: room, principal: alice, role: "owner" }]
          })
          const initial = yield* server.pull(alice, { epoch: null, cursors: {}, incremental: true })
          expect(entries(initial)).toHaveLength(2048)
          expect(initial.page?.next).toBe(2048)
          const all = [...entries(initial)]
          const edit: Wire.ExecuteRequest = {
            epoch: initial.epoch,
            clientId: "device",
            batches: [
              { id: "edit", commands: [{ aggregate: "Tag", command: { _tag: "RemoveTag", target: "row-2000" } }] }
            ]
          }
          const receipt = yield* server.execute(alice, edit)
          expect(yield* server.execute(alice, edit)).toEqual(receipt)
          let page = initial.page
          while (page?.next !== null && page?.next !== undefined) {
            const input = {
              epoch: initial.epoch,
              cursors: {},
              incremental: true,
              continuation: { id: page.id, offset: page.next }
            }
            const next = yield* server.pull(alice, input)
            expect(yield* server.pull(alice, input)).toEqual(next)
            expect(next.revision).toBe(initial.revision)
            expect(entries(next).length).toBeLessThanOrEqual(2048)
            all.push(...entries(next))
            page = next.page
          }
          expect(all).toHaveLength(2501)
          expect(Option.getOrUndefined(Arr.findFirst(all, (entry) => entry.id === "row-2000"))).toMatchObject({
            state: { removed: false }
          })
          const delta = yield* server.pull(alice, {
            epoch: initial.epoch,
            cursors: cursors(initial),
            incremental: true
          })
          expect(entries(delta)).toEqual([
            {
              _tag: "Upsert",
              aggregate: "Tag",
              id: "row-2000",
              seq: 2,
              state: { name: "name-2000", scope: "user:alice", removed: true }
            }
          ])
          expect(Arr.every(delta.scopes, (scope) => !scope.reset)).toBe(true)
          const legacy = yield* server.pull(alice, { epoch: initial.epoch, cursors: cursors(initial) })
          expect(entries(legacy)).toHaveLength(2501)
          expect(legacy.page).toBeUndefined()
          expect(
            entries(yield* server.pull(alice, { epoch: delta.epoch, cursors: cursors(delta), incremental: true }))
          ).toEqual([])
          yield* server.compactScopes(delta.revision)
          expect(
            Option.getOrUndefined(
              Arr.findFirst(
                (yield* server.pull(alice, {
                  epoch: initial.epoch,
                  cursors: cursors(initial),
                  incremental: true
                })).scopes,
                (s) => s.scope === "user:alice"
              )
            )?.reset
          ).toBe(true)
          expect(
            entries(yield* server.pull(alice, { epoch: delta.epoch, cursors: cursors(delta), incremental: true }))
          ).toEqual([])
          expect(
            (yield* server.execute(alice, {
              epoch: delta.epoch,
              clientId: "device",
              batches: [
                {
                  id: "move",
                  commands: [{ aggregate: "Tag", command: { _tag: "MoveTag", target: "row-2000", scope: room } }]
                }
              ]
            })).receipts[0]?._tag
          ).toBe("Accepted")
          const moved = yield* server.pull(alice, { epoch: delta.epoch, cursors: cursors(delta), incremental: true })
          expect(Option.getOrUndefined(Arr.findFirst(moved.scopes, (s) => s.scope === "user:alice"))?.entries).toEqual([
            { _tag: "Gone", aggregate: "Tag", id: "row-2000" }
          ])
          expect(Option.getOrUndefined(Arr.findFirst(moved.scopes, (s) => s.scope === room))?.entries[0]).toMatchObject(
            {
              _tag: "Upsert",
              id: "row-2000",
              state: { scope: room }
            }
          )
          const mutateMembership = (enabled: boolean) =>
            server.execute(alice, {
              epoch: null,
              clientId: "device",
              batches: [
                { id: `membership-${enabled}`, commands: [], action: { name: "membership", input: { enabled } } }
              ]
            })
          yield* mutateMembership(false)
          expect(
            (yield* Effect.result(
              server.pull(alice, {
                epoch: initial.epoch,
                cursors: {},
                incremental: true,
                continuation: { id: initial.page?.id ?? "", offset: 2048 }
              })
            ))._tag
          ).toBe("Failure")
          const revoked = yield* server.pull(alice, { epoch: delta.epoch, cursors: cursors(moved), incremental: true })
          expect(Arr.some(revoked.scopes, (s) => s.scope === room)).toBe(false)
          yield* mutateMembership(true)
          const granted = yield* server.pull(alice, { epoch: delta.epoch, cursors: cursors(moved), incremental: true })
          expect(Option.getOrUndefined(Arr.findFirst(granted.scopes, (s) => s.scope === room))).toMatchObject({
            reset: true,
            entries: [{ _tag: "Upsert", id: "row-2000" }]
          })
          yield* server.ingestCatalog({
            scope: "catalog:deletes",
            release: "one",
            rows: [
              { aggregate: "Tag", id: "deleted", state: { name: "deleted", scope: "catalog:deletes", removed: false } }
            ]
          })
          const catalog = yield* server.pull(alice, { epoch: null, cursors: cursors(granted), incremental: true })
          yield* server.ingestCatalog({ scope: "catalog:deletes", release: "empty", rows: [] })
          const removed = yield* server.pull(alice, { epoch: null, cursors: cursors(catalog), incremental: true })
          expect(
            Option.getOrUndefined(Arr.findFirst(removed.scopes, (s) => s.scope === "catalog:deletes"))?.entries
          ).toEqual([{ _tag: "Gone", aggregate: "Tag", id: "deleted" }])
          yield* sql`UPDATE echosync_pull_sessions SET expires_at=now()-interval '1 second'`
          expect(
            (yield* Effect.result(
              server.pull(alice, {
                epoch: removed.epoch,
                cursors: {},
                incremental: true,
                continuation: { id: removed.page?.id ?? "", offset: 0 }
              })
            ))._tag
          ).toBe("Failure")
          expect(
            Option.getOrUndefined(
              Arr.findFirst(
                (yield* server.pull(alice, {
                  epoch: null,
                  cursors: { "user:alice": Number.MAX_SAFE_INTEGER },
                  incremental: true
                })).scopes,
                (s) => s.scope === "user:alice"
              )
            )?.reset
          ).toBe(true)
        }),
        12,
        [membership]
      )
    },
    60000
  )

  it.effect(
    "INV-SCHEMA-4 deployment cannot rewrite historical facts or reinterpret old requests during projection rebuild",
    () =>
      scenario(
        Effect.fnUntraced(function* (_, sql) {
          const original = tallyBatch("original", { _tag: "AddTally", target: "one", pairs: 3 })
          const old = yield* Server.make({ aggregates: [LegacyTally] })
          const accepted = yield* old.execute(alice, original)
          const current = yield* Server.make({ aggregates: [CurrentTally] })
          yield* current.execute(alice, tallyBatch("offline", { _tag: "AddTally", target: "one", pairs: 2 }))
          yield* current.execute(alice, tallyBatch("new", { _tag: "AddTally", target: "one", units: 5, _version: 2 }))
          expect(yield* current.get(alice, CurrentTally, "one")).toEqual({ value: 15, scope: "user:alice" })
          expect(yield* old.get(alice, LegacyTally, "one")).toEqual({ value: 15, scope: "user:alice" })
          const facts = yield* sql`SELECT * FROM echosync_events ORDER BY seq`
          const receipts = yield* sql`SELECT * FROM echosync_receipts ORDER BY batch_id`
          const before = yield* current.pull(alice, { epoch: null, cursors: {} })
          const staged = yield* current.pull(alice, { epoch: before.epoch, cursors: {}, incremental: true })
          const projected = yield* Server.make({ aggregates: [ProjectedTally] })
          expect(yield* projected.get(alice, ProjectedTally, "one").pipe(Effect.flip)).toMatchObject({
            _tag: "ServerError"
          })
          expect(yield* projected.rebuildProjections([ProjectedTally])).toBe(1)
          expect(
            (yield* Effect.result(
              projected.pull(alice, {
                epoch: before.epoch,
                cursors: {},
                incremental: true,
                continuation: { id: staged.page?.id ?? "", offset: 0 }
              })
            ))._tag
          ).toBe("Failure")
          expect(yield* projected.get(alice, ProjectedTally, "one")).toEqual({ value: 15, scope: "user:alice" })
          expect(yield* sql`SELECT * FROM echosync_events ORDER BY seq`).toEqual(facts)
          expect(yield* sql`SELECT * FROM echosync_receipts ORDER BY batch_id`).toEqual(receipts)
          const after = yield* projected.pull(alice, {
            epoch: before.epoch,
            cursors: cursors(before),
            incremental: true
          })
          expect(after.scopes[0]?.reset).toBe(true)
          expect(entries(after)).toEqual([
            {
              _tag: "Upsert",
              aggregate: "Tally",
              id: "one",
              seq: 3,
              state: { _projection: 2, state: { value: 15, scope: "user:alice" } }
            }
          ])
          expect(after.revision).toBe(before.revision + 1)
          // Receipt recovery must work even after the entire command registry has been removed.
          const retired = yield* Server.make({ aggregates: [] })
          expect(yield* retired.execute(alice, original)).toEqual(accepted)
          const rows = yield* sql`SELECT * FROM echosync_streams`
          yield* sql`UPDATE echosync_events SET seq=9 WHERE seq=3`
          expect(yield* projected.rebuildProjections([ProjectedTally]).pipe(Effect.flip)).toMatchObject({
            _tag: "ServerError"
          })
          expect(yield* sql`SELECT * FROM echosync_streams`).toEqual(rows)
          yield* sql`UPDATE echosync_events SET seq=3 WHERE seq=9`
          // A later invalid stream must roll back a genuinely changed earlier projection and its cursors.
          yield* sql`UPDATE echosync_streams SET state='{"stale":true}' WHERE id='one'`
          const staleRows = yield* sql`SELECT * FROM echosync_streams`
          const engineBefore = yield* sql`SELECT * FROM echosync_engine`
          const scopesBefore = yield* sql`SELECT * FROM echosync_scopes`
          yield* sql`INSERT INTO echosync_streams VALUES ('Tally','z-catalog','catalog:tally',1,'null')`
          expect(yield* projected.rebuildProjections([ProjectedTally]).pipe(Effect.flip)).toMatchObject({
            _tag: "ServerError"
          })
          expect(yield* sql`SELECT * FROM echosync_streams WHERE id='one'`).toEqual(staleRows)
          expect(yield* sql`SELECT * FROM echosync_engine`).toEqual(engineBefore)
          expect(yield* sql`SELECT * FROM echosync_scopes`).toEqual(scopesBefore)
          yield* sql`DELETE FROM echosync_streams WHERE id='z-catalog'`
          yield* sql`UPDATE echosync_streams SET scope='user:bob' WHERE id='one'`
          expect(yield* projected.rebuildProjections([ProjectedTally]).pipe(Effect.flip)).toMatchObject({
            _tag: "ServerError"
          })
          expect(yield* sql`SELECT * FROM echosync_events ORDER BY seq`).toEqual(facts)
          const imported = {
            id: "imported",
            principal: alice,
            streams: [
              Server.importedStream(LegacyTally, "imported", [
                new OldTallyAdded({ pairs: 4, scope: privateScope(alice) })
              ])
            ]
          }
          yield* old.importStreams(imported)
          const beforeRetry = yield* sql`SELECT * FROM echosync_events ORDER BY id,seq`
          const importIdentity = yield* sql`SELECT * FROM echosync_imports`
          const retry = {
            ...imported,
            streams: [
              Server.importedStream(CurrentTally, "imported", [
                new TallyAdded({ units: 8, scope: privateScope(alice) })
              ])
            ]
          }
          yield* current.importStreams(retry)
          expect(yield* sql`SELECT * FROM echosync_events ORDER BY id,seq`).toEqual(beforeRetry)
          expect(yield* sql`SELECT * FROM echosync_imports`).toEqual(importIdentity)
          expect(
            yield* current
              .importStreams({
                ...retry,
                streams: [
                  Server.importedStream(CurrentTally, "imported", [
                    new TallyAdded({ units: 9, scope: privateScope(alice) })
                  ])
                ]
              })
              .pipe(Effect.flip)
          ).toMatchObject({ _tag: "ServerError" })
        })
      )
  )
  it.effect("INV-SERVER-11 naming cutover cannot split rows, reset sequences or partially commit", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        const migrate = Server.migrate.pipe(Effect.provideService(SqlClient.SqlClient, sql))
        const accepted = yield* server.execute(alice, request("retained"))
        yield* server.importStreams({
          id: "retained-import",
          principal: alice,
          streams: [
            Server.importedStream(Tag, TagId.make("shared"), [
              new TagHistoryImported({
                snapshot: { name: "shared", scope: VisibilityScope.make("shared:room"), removed: false }
              })
            ])
          ],
          grants: [{ scope: VisibilityScope.make("shared:room"), principal: bob, role: "reader" }]
        })
        yield* server.ingestCatalog({ scope: "catalog:tags", release: "retained", rows: [] })
        yield* sql`INSERT INTO echosync_connections VALUES ('retained-connection','alice',now()+interval '1 hour')`
        yield* sql`INSERT INTO echosync_presence VALUES ('retained-connection','user:alice','tab','{}',now()+interval '1 hour')`
        yield* sql`CREATE INDEX echosync_events_batch ON echosync_events(batch_id)`
        const tables = [
          "engine",
          "scopes",
          "streams",
          "events",
          "receipts",
          "catalog_releases",
          "grants",
          "grant_events",
          "imports",
          "connections",
          "presence"
        ]
        const snapshots = new Map<string, unknown>()
        for (const suffix of tables) {
          snapshots.set(
            suffix,
            yield* sql`SELECT to_jsonb(t) AS row FROM ${sql(`echosync_${suffix}`)} t ORDER BY to_jsonb(t)::text`
          )
          // Reconstruct the previous released naming, including dependent objects, without changing any data.
          const constraints = yield* sql<{
            readonly conname: string
          }>`SELECT conname FROM pg_constraint WHERE conrelid=${`echosync_${suffix}`}::regclass`
          for (const { conname } of constraints)
            yield* sql`ALTER TABLE ${sql(`echosync_${suffix}`)} RENAME CONSTRAINT ${sql(conname)} TO ${sql(Str.replace(/^echosync_/, "echo_")(conname))}`
          yield* sql`ALTER TABLE ${sql(`echosync_${suffix}`)} RENAME TO ${sql(`echo_${suffix}`)}`
        }
        yield* sql`ALTER INDEX echosync_events_batch RENAME TO echo_events_batch`
        yield* sql`ALTER SEQUENCE echosync_grant_events_position_seq RENAME TO echo_grant_events_position_seq`
        // Collision appears after earlier tables in migration order: every prior rename must roll back.
        yield* sql`CREATE TABLE echosync_events (sentinel text)`
        yield* sql`INSERT INTO echosync_events VALUES ('do-not-merge')`
        expect((yield* Effect.result(migrate))._tag).toBe("Failure")
        for (const suffix of tables)
          expect(
            yield* sql`SELECT to_jsonb(t) AS row FROM ${sql(`echo_${suffix}`)} t ORDER BY to_jsonb(t)::text`
          ).toEqual(snapshots.get(suffix))
        expect(yield* sql`SELECT * FROM echosync_events`).toEqual([{ sentinel: "do-not-merge" }])
        yield* sql`DROP TABLE echosync_events`
        // App migrations wrap this effect: a successful inner migration must not commit its caller.
        expect(
          (yield* Effect.result(sql.withTransaction(migrate.pipe(Effect.andThen(Effect.fail("outer rollback"))))))._tag
        ).toBe("Failure")
        expect(yield* sql`SELECT count(*)::integer AS count FROM echo_receipts`).toEqual([{ count: 1 }])
        yield* migrate
        yield* migrate
        for (const suffix of tables)
          expect(
            yield* sql`SELECT to_jsonb(t) AS row FROM ${sql(`echosync_${suffix}`)} t ORDER BY to_jsonb(t)::text`
          ).toEqual(snapshots.get(suffix))
        expect(
          yield* sql`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND starts_with(c.relname,'echo_')`
        ).toEqual([])
        expect(
          yield* sql`SELECT conname FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND starts_with(conname,'echo_')`
        ).toEqual([])
        expect(yield* sql`SELECT pg_get_serial_sequence('echosync_grant_events','position') AS name`).toEqual([
          {
            name: `${(yield* sql<{ readonly name: string }>`SELECT current_schema() AS name`)[0]?.name}.echosync_grant_events_position_seq`
          }
        ])
        expect(yield* sql`SELECT nextval('echosync_grant_events_position_seq')::integer AS position`).toEqual([
          { position: 2 }
        ])
        expect(yield* server.execute(alice, request("retained"))).toEqual(accepted)
        expect(yield* server.get(alice, Tag, TagId.make("retained"))).toMatchObject({ name: "retained" })
      })
    )
  )

  it.effect("INV-SERVER-7 reader grants cannot write and moves evict old readers", () =>
    scenario(
      Effect.fnUntraced(function* (server) {
        const shared = VisibilityScope.make("shared:room")
        const id = TagId.make("shared-tag")
        // Another application instance must wake a reader absent from the old grant index.
        const remote = yield* Server.make({ aggregates: [Tag, Note] })
        const notifications = yield* Queue.unbounded<Wire.Change>()
        yield* remote.changes(bob).pipe(
          Stream.filter((frame) => frame._tag === "Poke"),
          Stream.runForEach((frame) => Queue.offer(notifications, frame)),
          Effect.forkScoped
        )
        yield* Queue.take(notifications)
        yield* server.importStreams({
          id: "seed",
          principal: alice,
          streams: [
            Server.importedStream(Tag, id, [
              new TagHistoryImported({ snapshot: { name: "shared", scope: shared, removed: false } })
            ])
          ],
          grants: [
            { scope: shared, principal: alice, role: "owner" },
            { scope: shared, principal: bob, role: "reader" }
          ]
        })
        yield* Queue.take(notifications)
        const before = yield* server.pull(bob, { epoch: null, cursors: {} })
        expect(entries(before)).toHaveLength(1)
        expect((yield* server.execute(bob, note("read", id))).receipts[0]?._tag).toBe("Accepted")
        yield* Queue.take(notifications)
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
        yield* Queue.take(notifications)
        const after = yield* server.pull(bob, { epoch: null, cursors: cursors(before) })
        expect(Arr.findFirst(after.scopes, (scope) => scope.scope === shared)).toMatchObject({
          value: { reset: true, entries: [] }
        })
        expect(yield* server.get(bob, Tag, id)).toBeNull()
        expect(yield* server.get(alice, Tag, id)).toMatchObject({ scope: privateScope(alice) })

        // A source-only gate would accept an owner transferring into a merely writable scope.
        const writable = VisibilityScope.make("shared:writable")
        yield* server.importStreams({
          id: "roles",
          principal: alice,
          streams: [],
          grants: [
            { scope: writable, principal: alice, role: "writer" },
            { scope: writable, principal: bob, role: "owner" }
          ]
        })
        expect((yield* server.execute(alice, transfer("writer-destination", id, writable))).receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "ScopeForbidden" }
        })
        expect(yield* server.get(alice, Tag, id)).toMatchObject({ scope: privateScope(alice) })
        expect((yield* server.execute(bob, request("bob-owned"))).receipts[0]?._tag).toBe("Accepted")
        expect(
          (yield* server.execute(bob, transfer("owner-destination", "bob-owned", writable))).receipts[0]?._tag
        ).toBe("Accepted")
        const visible = yield* server.get(alice, Tag, TagId.make("bob-owned"))
        const writerGrants: ReadonlyArray<Grant> = [{ scope: writable, principal: alice, role: "writer" }]
        const inspected = capabilities(Tag, visible, alice, writerGrants)
        expect(inspected).toMatchObject({ role: "writer", read: true })
        expect(inspected.commands).toContainEqual({
          tag: "MoveTag",
          access: "owner",
          authority: "both",
          allowed: false
        })
        expect(inspected.commands).toContainEqual({
          tag: "RemoveTag",
          access: "writer",
          authority: "both",
          allowed: true
        })
        expect(inspected.commands).toContainEqual({
          tag: "RelocateTag",
          access: "writer",
          authority: "server",
          allowed: false
        })
        expect(capabilities(Tag, null, alice).commands).toContainEqual({
          tag: "DefineTag",
          access: "writer",
          authority: "both",
          allowed: null
        })
        expect(
          (yield* server.execute(alice, transfer("writer-source", "bob-owned", privateScope(alice)))).receipts[0]
        ).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "ScopeForbidden" }
        })
        // Default writer access still accepts a real server command; owner is a tightening, not a new default.
        expect(
          (yield* server.execute(alice, {
            epoch: null,
            clientId: "device",
            batches: [
              {
                id: "writer-remove",
                commands: [{ aggregate: "Tag", command: { _tag: "RemoveTag", target: "bob-owned" } }]
              }
            ]
          })).receipts[0]?._tag
        ).toBe("Accepted")
        expect(yield* server.get(bob, Tag, TagId.make("bob-owned"))).toMatchObject({ removed: true })
        const ownerGrants: ReadonlyArray<Grant> = [{ scope: writable, principal: bob, role: "owner" }]
        expect(capabilities(Tag, visible, bob, ownerGrants).commands).toContainEqual({
          tag: "MoveTag",
          access: "owner",
          authority: "both",
          allowed: true
        })
        expect(
          (yield* server.execute(alice, {
            epoch: null,
            clientId: "device",
            batches: [{ id: "revoke-owner", commands: [], action: { name: "revoke-owner", input: {} } }]
          })).receipts[0]?._tag
        ).toBe("Accepted")
        // The stale capability remains true, but admission reloads grants and denies the old owner.
        expect(
          (yield* server.execute(bob, transfer("revoked-owner", "bob-owned", privateScope(bob)))).receipts[0]
        ).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "ScopeForbidden" }
        })
        expect(yield* server.get(bob, Tag, TagId.make("bob-owned"))).toBeNull()
        expect(
          Arr.some((yield* server.pull(bob, { epoch: null, cursors: {} })).scopes, (scope) => scope.scope === writable)
        ).toBe(false)
      }),
      12,
      [
        Server.Action.make({
          name: "revoke-owner",
          input: Schema.Struct({}),
          reads: [],
          decide: (_, ctx) =>
            ctx.principal === alice
              ? { operations: [Server.Action.revoke(VisibilityScope.make("shared:writable"), bob)] }
              : { rejection: { _tag: "Denied" } }
        })
      ]
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
        expect((yield* server.execute(alice, raw)).receipts[0]).toMatchObject({
          _tag: "Rejected",
          rejection: { _tag: "ServerOnly" }
        })
        const rejected = {
          epoch: null,
          clientId: "device",
          batches: [{ id: "bad", commands: [], action: { name: "transition", input: { fail: true } } }]
        }
        expect((yield* server.execute(alice, rejected)).receipts[0]?._tag).toBe("Rejected")
        expect(yield* sql`SELECT * FROM echosync_grants`).toHaveLength(0)
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
        expect(yield* sql`SELECT * FROM echosync_events WHERE id=${id}`).toHaveLength(2)
        const pulled = yield* server.pull(alice, { epoch: null, cursors: {} })
        const receipt = result.receipts[0]
        expect(receipt?._tag === "Accepted" && pulled.revision >= receipt.revision).toBe(true)

        // An action is not blanket authority for the ordinary commands it stages.
        yield* server.importStreams({
          id: "writer-role",
          principal: alice,
          streams: [],
          grants: [{ scope: shared, principal: alice, role: "writer" }]
        })
        expect((yield* server.execute(alice, request("ordinary"))).receipts[0]?._tag).toBe("Accepted")
        for (const [target, destination] of [
          [id, privateScope(alice)],
          [TagId.make("ordinary"), shared]
        ] as const) {
          expect(
            (yield* server.execute(alice, {
              epoch: null,
              clientId: "device",
              batches: [
                {
                  id: `ordinary-${target}`,
                  commands: [],
                  action: { name: "ordinary-transfer", input: { target, destination } }
                }
              ]
            })).receipts[0]
          ).toMatchObject({ _tag: "Rejected", rejection: { _tag: "ScopeForbidden" } })
        }
        expect(yield* sql`SELECT * FROM echosync_grants WHERE scope='shared:leak'`).toHaveLength(0)
        expect(yield* sql`SELECT * FROM echosync_events`).toHaveLength(3)
        expect(yield* server.get(alice, Tag, id)).toMatchObject({ scope: shared })
        expect(yield* server.get(alice, Tag, TagId.make("ordinary"))).toMatchObject({ scope: privateScope(alice) })
      }),
      12,
      [
        action,
        Server.Action.make({
          name: "ordinary-transfer",
          input: Schema.Struct({ target: TagId, destination: VisibilityScope }),
          reads: [],
          decide: ({ target, destination }, ctx) =>
            ctx.principal === alice
              ? {
                  operations: [
                    Server.Action.grant({ scope: VisibilityScope.make("shared:leak"), principal: bob, role: "reader" }),
                    Server.Action.command(Tag, new MoveTag({ target, scope: destination }))
                  ]
                }
              : { rejection: { _tag: "Denied" } }
        })
      ]
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
        expect(yield* sql`SELECT payload,principal FROM echosync_events`).toMatchObject([
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
        // No write/notification is needed to discover a crashed participant's expired lease.
        yield* sql`UPDATE echosync_presence SET expires_at=now()-interval '1 second' WHERE client_id='tab'`
        yield* TestClock.adjust("25 seconds")
        expect((yield* Queue.take(queue)).presence).toEqual([])
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
        expect(yield* sql`SELECT * FROM echosync_events`).toHaveLength(1)
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
        yield* sql`INSERT INTO echosync_events VALUES ('Tag','imported',1,${sql.json(payload)},'trusted-import','operator',now())`
        yield* sql`INSERT INTO echosync_streams VALUES ('Tag','imported','user:alice',1,${sql.json(state)})`
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
        expect(yield* sql`SELECT state FROM echosync_streams WHERE id='imported'`).toEqual([
          { state: { name: "historical", scope: "user:alice", removed: true } }
        ])
        expect(yield* sql`SELECT seq,payload FROM echosync_events WHERE id='imported' ORDER BY seq`).toEqual([
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
        expect(yield* sql`SELECT * FROM echosync_events WHERE id='illegal-import'`).toHaveLength(0)
        expect(yield* sql`SELECT * FROM echosync_receipts WHERE batch_id='illegal-import'`).toHaveLength(0)

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
        expect(yield* sql`SELECT * FROM echosync_streams WHERE id IN ('in-batch','in-batch-note')`).toHaveLength(0)
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
        expect(yield* sql`SELECT state->'removed' AS removed FROM echosync_streams WHERE id='ephemeral'`).toEqual([
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
          yield* sql`SELECT seq,state->'removed' AS removed FROM echosync_streams WHERE id='persisted-overlay'`
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
        expect(yield* sql`SELECT * FROM echosync_events`).toHaveLength(2)
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
        expect(yield* sql`SELECT * FROM echosync_events`).toHaveLength(2)
      })
    )
  )

  it.effect("INV-SERVER-3 hidden targets cannot be overwritten and reads include catalogs but not other users", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        // Extra wire fields never become domain inputs or replace authenticated identity.
        expect(
          (yield* server.execute(alice, {
            epoch: null,
            clientId: "device",
            batches: [
              {
                id: "protected-input",
                commands: [
                  {
                    aggregate: "Tag",
                    command: {
                      _tag: "DefineTag",
                      target: "protected-input",
                      name: "protected",
                      scope: "user:bob",
                      principal: "bob",
                      authority: "server",
                      access: "reader",
                      grants: [{ scope: "user:bob", principal: "alice", role: "owner" }]
                    }
                  }
                ]
              }
            ]
          })).receipts[0]?._tag
        ).toBe("Accepted")
        expect(yield* server.get(alice, Tag, TagId.make("protected-input"))).toEqual({
          name: "protected",
          scope: privateScope(alice),
          removed: false
        })
        expect(yield* server.get(bob, Tag, TagId.make("protected-input"))).toBeNull()
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
        // Privileged action reads do not widen a server-only command's declared reads.
        const trusted = yield* Server.make({
          aggregates: [Tag, TrustedNote],
          actions: [
            Server.Action.make({
              name: "trusted-note",
              input: Schema.Struct({ tagId: TagId }),
              reads: [Tag],
              decide: ({ tagId }, ctx) =>
                ctx.read.get(Tag, tagId) === null
                  ? { rejection: { _tag: "PlannerMissingTag" } }
                  : {
                      operations: [
                        Server.Action.command(
                          TrustedNote,
                          new TrustedWriteNote({
                            target: NoteId.make(`trusted-${tagId}`),
                            tagId,
                            text: "trusted read"
                          })
                        )
                      ]
                    }
            })
          ]
        })
        for (const tagId of ["hidden", "catalog-tag"]) {
          const result = yield* trusted.execute(alice, {
            epoch: null,
            clientId: "device",
            batches: [{ id: `trusted-${tagId}`, commands: [], action: { name: "trusted-note", input: { tagId } } }]
          })
          expect(result.receipts[0]).toMatchObject(
            tagId === "hidden" ? { _tag: "Rejected", rejection: { _tag: "UnknownTag" } } : { _tag: "Accepted" }
          )
        }
        expect(yield* server.get(alice, Note, NoteId.make("trusted-hidden"))).toBeNull()
        expect(yield* server.get(alice, Note, NoteId.make("trusted-catalog-tag"))).toMatchObject({
          tagId: "catalog-tag"
        })
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
          failure: { _tag: "ServerError", message: "EchoSync storage operation failed" }
        })
        expect(diagnostics).toHaveLength(1)
        expect(diagnostics[0]).toContain("undeclared read")

        // Removing a tag reads Notes, not other Tags. Unrelated corrupt state must not be decoded.
        yield* server.execute(alice, request("isolated-target"))
        yield* sql`INSERT INTO echosync_streams VALUES ('Tag','unrelated','catalog:tags',1,'{}'::jsonb)`
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
        expect(yield* sql`SELECT state FROM echosync_streams WHERE id='unrelated'`).toEqual([{ state: {} }])
        // The same malformed shape must still fail when it belongs to a declared read aggregate.
        yield* sql`INSERT INTO echosync_streams VALUES ('Note','selected','catalog:notes',1,'{}'::jsonb)`
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
        expect(yield* sql`SELECT * FROM echosync_receipts WHERE batch_id='selected-invalid'`).toHaveLength(0)
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
        expect(yield* sql`SELECT * FROM echosync_events`).toHaveLength(1)
        expect(yield* sql`SELECT * FROM echosync_catalog_releases`).toHaveLength(1)
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
        expect(yield* sql`SELECT * FROM echosync_catalog_releases`).toHaveLength(1)
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
        expect(yield* sql`SELECT * FROM echosync_catalog_releases`).toHaveLength(2)
      })
    )
  )

  it.effect("INV-SERVER-10 readers share a listener without blocking peers or poking unrelated scopes", () =>
    scenario(
      Effect.fnUntraced(function* (_server, sql) {
        // A discarded constructor must not keep reconnecting in its caller's application scope.
        let blockAcquisition = false
        const acquiring = yield* Deferred.make<void>()
        const releaseAcquisition = yield* Deferred.make<void>()
        const name = `${sql.config.startupParameters?.["application_name"]}-acquire`
        const parameters: Record<string, string> = { ...sql.config.startupParameters, echosync_invalid_setting: "on" }
        const acquisitionClient = yield* PgClient.make({
          ...sql.config,
          maxConnections: 3,
          applicationName: name,
          startupParameters: parameters,
          onAcquire: Effect.gen(function* () {
            if (blockAcquisition) {
              yield* Deferred.succeed(acquiring, undefined)
              yield* Deferred.await(releaseAcquisition)
            }
          })
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- independent database client for real connection-acquisition fault injection
        }).pipe(Effect.provide(Reactivity.layer))
        const construct = Server.make({ aggregates: [Tag, Note] }).pipe(
          Effect.provideService(PgClient.PgClient, acquisitionClient)
        )
        expect((yield* Effect.result(construct))._tag).toBe("Failure")
        delete parameters["echosync_invalid_setting"]
        yield* TestClock.adjust("2 seconds")
        expect(yield* sql`SELECT pid FROM pg_stat_activity WHERE application_name=${name}`).toHaveLength(0)
        blockAcquisition = true
        const cancelled = yield* construct.pipe(Effect.forkScoped)
        yield* Deferred.await(acquiring)
        yield* Fiber.interrupt(cancelled)
        blockAcquisition = false
        yield* Deferred.succeed(releaseAcquisition, undefined)
        expect(yield* sql`SELECT pid FROM pg_stat_activity WHERE application_name=${name}`).toHaveLength(0)
        const recoveredScope = yield* Scope.fork(yield* Effect.scope)
        yield* Scope.provide(construct, recoveredScope)
        expect(yield* sql`SELECT pid FROM pg_stat_activity WHERE application_name=${name}`).toHaveLength(1)
        yield* Scope.close(recoveredScope, Exit.void)
        const owned = yield* Scope.fork(yield* Effect.scope)
        const server = yield* Scope.provide(Server.make({ aggregates: [Tag, Note] }), owned)
        const listeners = sql<{
          readonly count: number
        }>`SELECT count(*)::int AS count FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND query LIKE 'LISTEN %'`
        expect((yield* listeners)[0]?.count).toBe(2)
        const open = Effect.fnUntraced(function* (principal: Principal) {
          const queue = yield* Queue.unbounded<Wire.Change>()
          const fiber = yield* server.changes(principal).pipe(
            Stream.runForEach((frame) => Queue.offer(queue, frame)),
            Effect.result,
            Effect.forkScoped
          )
          const initial = yield* Queue.take(queue)
          expect(initial._tag).toBe("Poke")
          return { queue, fiber, initial }
        })
        const readers = yield* Effect.all(
          Arr.makeBy(24, (i) => open(i === 0 ? alice : Principal.make(`other-${i}`))),
          { concurrency: "unbounded" }
        )
        const first = readers[0]
        const unrelated = readers[1]
        if (first === undefined || unrelated === undefined) return yield* Effect.die("Missing readers")
        expect((yield* listeners)[0]?.count).toBe(2)
        const resume = yield* Deferred.make<void>()
        const paused = yield* Queue.unbounded<Wire.Change>()
        let frames = 0
        const slow = yield* server.changes(alice).pipe(
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              yield* Queue.offer(paused, frame)
              if (++frames === 1) yield* Deferred.await(resume)
            })
          ),
          Effect.result,
          Effect.forkScoped
        )
        yield* Queue.take(paused)
        for (const reader of readers) yield* Queue.clear(reader.queue)
        const nextPoke = Effect.fnUntraced(function* (queue: Queue.Dequeue<Wire.Change>) {
          while (true) {
            const frame = yield* Queue.take(queue)
            if (frame._tag === "Poke") return frame
          }
        })
        for (let i = 0; i < 8; i++) {
          yield* server.execute(alice, request(`shared-listener-${i}`))
          yield* nextPoke(first.queue)
          yield* sql`SELECT pg_notify('echosync_changes','presence')`
        }
        // A presence refresh after the data refresh must not overwrite slow reader's sticky Poke.
        yield* Queue.take(first.queue)
        expect(frames).toBe(1)
        expect(Arr.some(yield* Queue.clear(unrelated.queue), (frame) => frame._tag === "Poke")).toBe(false)
        yield* Deferred.succeed(resume, undefined)
        expect((yield* Queue.take(paused))._tag).toBe("Poke")
        expect(entries(yield* server.pull(alice, { epoch: null, cursors: {} }))).toHaveLength(8)
        expect(entries(yield* server.pull(bob, { epoch: null, cursors: {} }))).toHaveLength(0)
        yield* Fiber.interrupt(slow)
        yield* server.execute(alice, request("after-close"))
        yield* nextPoke(first.queue)
        // Hold a real snapshot after it has taken dirty state, then commit from another instance.
        const remoteScope = yield* Scope.fork(yield* Effect.scope)
        const remote = yield* Scope.provide(construct, remoteScope)
        const locked = yield* Deferred.make<void>()
        const unlock = yield* Deferred.make<void>()
        const locker = yield* acquisitionClient
          .withTransaction(
            Effect.gen(function* () {
              yield* acquisitionClient`LOCK TABLE echosync_presence IN ACCESS EXCLUSIVE MODE`
              yield* Deferred.succeed(locked, undefined)
              yield* Deferred.await(unlock)
            })
          )
          .pipe(Effect.forkScoped)
        yield* Deferred.await(locked)
        yield* Queue.clear(first.queue)
        yield* remote.execute(alice, request("race-first"))
        while (
          (yield* acquisitionClient`SELECT pid FROM pg_stat_activity WHERE application_name=${sql.config.startupParameters?.["application_name"]} AND wait_event_type='Lock' AND query LIKE 'SELECT p.room%'`)
            .length === 0
        )
          yield* Effect.yieldNow
        yield* remote.execute(alice, request("race-second"))
        const arriving = yield* server.changes(alice).pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped)
        yield* Deferred.succeed(unlock, undefined)
        yield* Fiber.join(locker)
        yield* nextPoke(first.queue)
        yield* nextPoke(first.queue)
        expect((yield* Fiber.join(arriving))[0]?._tag).toBe("Poke")
        expect(entries(yield* server.pull(alice, { epoch: null, cursors: {} }))).toHaveLength(11)
        yield* Scope.close(remoteScope, Exit.void)
        // Unknown notifications and accepted no-ops conservatively wake unrelated readers too.
        yield* Queue.clear(unrelated.queue)
        yield* sql`SELECT pg_notify('echosync_changes','unknown-future-format')`
        yield* nextPoke(unrelated.queue)
        yield* server.execute(alice, { epoch: null, clientId: "device", batches: [{ id: "no-op", commands: [] }] })
        yield* nextPoke(unrelated.queue)
        yield* sql`SELECT pg_notify('echosync_changes',(SELECT json_agg('unreadable:' || i)::text FROM generate_series(1,257) AS i))`
        yield* nextPoke(unrelated.queue)
        // A silent epoch change is discovered by shared maintenance, not a client poll.
        yield* Queue.clear(first.queue)
        yield* sql`UPDATE echosync_engine SET epoch='silent-reset'`
        yield* TestClock.adjust("25 seconds")
        expect((yield* Queue.take(first.queue)).epoch).toBe("silent-reset")
        // Expired connection tokens are not silently revived by batch maintenance.
        yield* sql`UPDATE echosync_connections SET expires_at=now()-interval '1 second' WHERE id=${unrelated.initial.connectionId}`
        yield* TestClock.adjust("25 seconds")
        expect((yield* Fiber.join(unrelated.fiber))._tag).toBe("Failure")
        // Exhaust the real query pool: refresh must fail by its deadline, not keep streams healthy.
        const blocked = yield* Scope.fork(yield* Effect.scope)
        yield* Scope.provide(sql.reserve, blocked)
        yield* acquisitionClient`SELECT pg_notify('echosync_changes','presence')`
        while ((yield* sql.poolStats).pending === 0) yield* Effect.yieldNow
        yield* TestClock.adjust("20 seconds")
        // Socket shutdown is real I/O; wait until the readers actually enter their cleanup deadline.
        while ((yield* sql.poolStats).pending < readers.length - 1) yield* Effect.yieldNow
        yield* TestClock.adjust("5 seconds")
        for (const reader of readers) expect((yield* Fiber.join(reader.fiber))._tag).toBe("Failure")
        yield* Scope.close(blocked, Exit.void)
        // Failed immediate cleanup is safe: lease expiry clears stale tokens on the next open.
        yield* sql`UPDATE echosync_connections SET expires_at=now()-interval '1 second'`
        yield* TestClock.adjust("1 second")
        const recovered = yield* server.changes(alice).pipe(
          Stream.take(1),
          Stream.runCollect,
          // Let real socket handshakes run before advancing virtual reconnect/connect deadlines again.
          Effect.tapError(() =>
            TestClock.withLive(Effect.sleep("50 millis")).pipe(Effect.andThen(TestClock.adjust("1 second")))
          ),
          Effect.retry({ times: 10 })
        )
        expect(recovered[0]?._tag).toBe("Poke")
        const liveAtShutdown = yield* open(alice)
        yield* Scope.close(owned, Exit.void)
        expect((yield* Fiber.join(liveAtShutdown.fiber))._tag).toBe("Failure")
        expect((yield* listeners)[0]?.count).toBe(1)
        expect(yield* sql`SELECT * FROM echosync_connections`).toHaveLength(0)
      }),
      1
    )
  )

  it.effect("INV-SERVER-6 HTTP RPC authenticates outside payload and changes reconnect with a catchup poke", () =>
    scenario(
      Effect.fnUntraced(function* (server, sql) {
        const web = HttpEffect.toWebHandler(
          Server.httpApp(server, (req) =>
            req.headers["authorization"] === "Bearer fixture"
              ? Effect.succeed(alice)
              : Effect.fail(new Wire.ServerError({ message: "Unauthorized" }))
          )
        )
        const context = yield* Effect.context()
        const listener = createServer((incoming, outgoing) => {
          const handle = Effect.gen(function* () {
            const chunks = yield* Schema.decodeEffect(Schema.Array(Schema.Uint8Array))(
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
            if (response.body !== null) {
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
        const obsolete = yield* Effect.promise(() =>
          web(
            new Request("http://localhost/sync", {
              method: "POST",
              headers: { authorization: "Bearer fixture" },
              body: "{}"
            })
          )
        )
        expect(obsolete.status).toBe(426)
        const unauthenticated = RpcClient.layerProtocolHttp({
          url: `http://127.0.0.1:${address.port}`,
          transformClient: (client) =>
            HttpClient.mapRequest(client, HttpClientRequest.setHeader("x-echosync-protocol", "2"))
        }).pipe(Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]))
        const denied = yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(Wire.EchoSyncRpcs)
          return yield* Effect.result(client.Execute(request("unauthorized")))
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- unauthenticated RPC test runtime
        }).pipe(Effect.provide(unauthenticated))
        expect(denied._tag).toBe("Failure")
        expect(entries(yield* server.pull(alice, { epoch: null, cursors: {} }))).toHaveLength(0)
        const protocol = RpcClient.layerProtocolHttp({
          url: `http://127.0.0.1:${address.port}`,
          transformClient: (client) =>
            HttpClient.mapRequest(
              client,
              HttpClientRequest.setHeaders({ authorization: "Bearer fixture", "x-echosync-protocol": "2" })
            )
        }).pipe(Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]))
        yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(Wire.EchoSyncRpcs)
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
          const connected = yield* Queue.unbounded<Wire.Change>()
          const interrupted = yield* client.Changes({}).pipe(
            Stream.runForEach((frame) => Queue.offer(connected, frame)),
            Effect.result,
            Effect.forkScoped
          )
          yield* Queue.take(connected)
          yield* sql`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND query LIKE 'LISTEN %'`
          expect((yield* Fiber.join(interrupted))._tag).toBe("Failure")
          yield* server.execute(alice, request("in-listener-gap"))
          yield* TestClock.adjust("1 second")
          const catchup = yield* client.Changes({}).pipe(Stream.take(1), Stream.runCollect, Effect.retry({ times: 10 }))
          expect(catchup[0]?._tag).toBe("Poke")
          expect(entries(yield* client.Pull({ epoch: null, cursors: {} }))).toHaveLength(2)
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- real RPC client runtime entry point
        }).pipe(Effect.provide(protocol))
      }),
      1
    )
  )
})
