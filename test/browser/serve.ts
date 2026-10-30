import * as PgClient from "@effect/sql-pg/PgClient"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"
import * as Str from "effect/String"
import { HttpEffect } from "effect/http"
import { Note, Tag } from "../../examples/fixture/domain/src/index.ts"
import { Principal } from "../../src/domain/Scope.ts"
import * as Server from "../../src/server/index.ts"

// Dedicated disposable schema, never app data; this runner only connects to the orb's local test database.
const postgres = PgClient.layer({
  url: Redacted.make("postgres://postgres@127.0.0.1:5432/postgres"),
  startupParameters: { search_path: "echo_browser_fixture" }
})
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* PgClient.PgClient
      yield* sql`DROP SCHEMA IF EXISTS echo_browser_fixture CASCADE`
      yield* sql`CREATE SCHEMA echo_browser_fixture`
      yield* Server.migrate
      const server = yield* Server.make({ aggregates: [Note, Tag] })
      const rpc = HttpEffect.toWebHandler(
        Server.httpApp(server, () => Effect.succeed(Principal.make("browser-fixture")))
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
                const name = "echo-browser-${script.hash}";
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
                    '<!doctype html><title>Echo browser scenarios</title><p>Client invariant scenario harness</p><script type="module">if (!navigator.serviceWorker.controller) { await navigator.serviceWorker.register("/shell-worker.js"); await navigator.serviceWorker.ready } await import("/client.js")</script>',
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
    // oxlint-disable-next-line effecttsgo/strict-effect-provide
  ).pipe(Effect.provide(postgres))
)
