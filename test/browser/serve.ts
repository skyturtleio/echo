import * as PgClient from "@effect/sql-pg/PgClient"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"
import * as Str from "effect/String"
import { HttpEffect } from "effect/http"
import { Note, Tag } from "../../examples/fixture/domain/src/index.ts"
import { TagHistoryImported } from "../../examples/fixture/domain/src/Tag/TagHistoryImported.event.ts"
import { TagId } from "../../examples/fixture/domain/src/Tag/Tag.id.ts"
import { Principal, Scope as SchemaScope } from "../../src/domain/Scope.ts"
import { CurrentTally } from "../../examples/fixture/domain/src/Evolution.ts"
import * as Server from "../../src/server/index.ts"

// Dedicated disposable schema, never app data; this runner only connects to the orb's local test database.
const postgres = PgClient.layer({
  url: Redacted.make("postgres://postgres@127.0.0.1:5432/postgres"),
  startupParameters: { search_path: "echosync_browser_fixture" }
})
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* PgClient.PgClient
      yield* sql`DROP SCHEMA IF EXISTS echosync_browser_fixture CASCADE`
      yield* sql`CREATE SCHEMA echosync_browser_fixture`
      yield* Server.migrate
      const server = yield* Server.make({ aggregates: [Note, Tag, CurrentTally] })
      const rpc = HttpEffect.toWebHandler(
        Server.httpApp(server, () => Effect.succeed(Principal.make("browser-fixture")))
      )
      const schemaRpc = HttpEffect.toWebHandler(
        Server.httpApp(server, () => Effect.succeed(Principal.make("schema-fixture")))
      )
      const run = Effect.runPromiseWith(yield* Effect.context())
      const seedCatalog = () =>
        run(
          server.ingestCatalog({
            scope: "catalog:transport",
            release: "large",
            rows: Array.from({ length: 84_780 }, (_, i) => ({
              aggregate: "Tag",
              id: `catalog:${i}`,
              state: { name: `${i}:${Str.repeat(380)("x")}`, removed: false, scope: "catalog:transport" }
            }))
          })
        )
      const build = yield* Effect.promise(() =>
        Bun.build({ entrypoints: ["test/browser/client.ts"], target: "browser" })
      )
      if (!build.success) return yield* Effect.die(new AggregateError(build.logs, "Browser scenario bundle failed"))
      const script = build.outputs[0]
      if (script === undefined) return yield* Effect.die(new Error("Missing browser scenario bundle"))
      let unauthorizedRequests = 0
      let oversizedRequests = 0
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          Bun.serve({
            port: 4178,
            fetch(request, listener) {
              const path = new URL(request.url).pathname
              if (path === "/sync" || path === "/sync/") return rpc(request)
              if (path === "/seed-incremental")
                return run(
                  Effect.gen(function* () {
                    yield* server.importStreams({
                      id: "incremental-fixture",
                      principal: Principal.make("browser-fixture"),
                      streams: Array.from({ length: 3001 }, (_, i) =>
                        Server.importedStream(Tag, TagId.make(`incremental-${i}`), [
                          new TagHistoryImported({
                            snapshot: {
                              name: `incremental-${i}`,
                              scope: SchemaScope.make("user:browser-fixture"),
                              removed: false
                            }
                          })
                        ])
                      )
                    })
                    yield* server.ingestCatalog({
                      scope: "catalog:incremental",
                      release: "one",
                      rows: [
                        {
                          aggregate: "Tag",
                          id: "incremental-deleted",
                          state: { name: "delete", scope: "catalog:incremental", removed: false }
                        }
                      ]
                    })
                  })
                ).then(() => Response.json(true))
              if (path === "/delete-incremental")
                return run(server.ingestCatalog({ scope: "catalog:incremental", release: "empty", rows: [] })).then(
                  () => Response.json(true)
                )
              if (path === "/schema-sync" || path === "/schema-sync/") return schemaRpc(request)
              if (path === "/seed-catalog") {
                // Administrative fixture ingestion can exceed Bun's idle timeout; RPC deadlines stay unchanged.
                listener.timeout(request, 0)
                return seedCatalog().then(() => Response.json(true))
              }
              if (path === "/oversized-count") return Response.json(oversizedRequests)
              if (path === "/oversized" || path === "/oversized/") {
                oversizedRequests++
                // Fault injection over real HTTP: fragmented unfinished NDJSON frame exceeds
                // the response cap without allocating a second enormous catalog on the server.
                let chunks = 0
                return new Response(
                  new ReadableStream({
                    pull(controller) {
                      if (chunks++ < 65) controller.enqueue(new TextEncoder().encode(Str.repeat(1024 * 1024)(" ")))
                      else {
                        controller.enqueue(new TextEncoder().encode("{}\n"))
                        controller.close()
                      }
                    }
                  }),
                  { headers: { "content-type": "application/ndjson" } }
                )
              }
              if (path === "/auth-count") return Response.json(unauthorizedRequests)
              if (path === "/unauthorized" || path === "/unauthorized/") {
                unauthorizedRequests++
                return new Response("Unauthorized", { status: 401 })
              }
              // Only the fixture app shell is cached. RPC is never intercepted or mocked by this worker.
              if (path === "/shell-worker.js")
                return new Response(
                  `
                const name = "echosync-browser-${script.hash}";
                self.addEventListener("install", event => event.waitUntil(caches.open(name).then(cache => cache.addAll(["/", "/client.js"])).then(() => self.skipWaiting())));
                self.addEventListener("activate", event => event.waitUntil(self.clients.claim()));
                self.addEventListener("fetch", event => {
                  if (event.request.method === "GET" && ["/", "/client.js"].includes(new URL(event.request.url).pathname))
                    event.respondWith(caches.open(name).then(cache => cache.match(event.request)).then(response => response || fetch(event.request)));
                });
              `,
                  { headers: { "content-type": "text/javascript" } }
                )
              return path === "/client.js"
                ? new Response(script, { headers: { "content-type": "text/javascript" } })
                : new Response(
                    '<!doctype html><title>EchoSync browser scenarios</title><p>Client invariant scenario harness</p><script type="module">if (!navigator.serviceWorker.controller) { await navigator.serviceWorker.register("/shell-worker.js"); await navigator.serviceWorker.ready } await import("/client.js")</script>',
                    { headers: { "content-type": "text/html" } }
                  )
            }
          })
        ),
        (listener) => Effect.promise(() => listener.stop(true))
      )
      return yield* Effect.never
    })
    // Standalone browser scenario server entry point.
  ).pipe(Effect.provide(postgres))
)
