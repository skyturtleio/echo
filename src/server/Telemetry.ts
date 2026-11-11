import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as References from "effect/References"
import * as Stream from "effect/Stream"
import * as Telemetry from "../Telemetry.ts"

/** Run in the request fiber so existing logger annotations and trace parents are retained. */
const start = Effect.fnUntraced(function* (
  operation: "Pull" | "Execute" | "Changes",
  page: "first" | "continuation" | null
) {
  const operationId = yield* Telemetry.randomId
  const began = yield* Clock.monotonicTimeNanos
  const span = yield* Effect.makeSpan(`EchoSync.operation.${operation}`, {
    attributes: { operationId, operation, page }
  }).pipe(
    Effect.provideService(References.TracerSpanAnnotations, {}),
    Effect.provideService(References.TracerSpanLinks, []),
    Effect.catchCause(() => Effect.succeed(null))
  )
  const log = (outcome: string, durationMs: number, failure: Telemetry.Failure | null, metrics: Telemetry.Metrics) =>
    Effect.logInfo("echosync.operation").pipe(
      Effect.annotateLogs({ operationId, operation, page, outcome, durationMs, failure, ...metrics }),
      Effect.ignoreCause
    )
  yield* log("started", 0, null, Telemetry.empty)
  return <A, E>(exit: Exit.Exit<A, E>, metrics: (value: A) => Telemetry.Metrics) =>
    Effect.gen(function* () {
      const end = yield* Clock.monotonicTimeNanos
      const failure = Exit.isFailure(exit) ? Telemetry.category(exit.cause) : null
      const outcome = Exit.isSuccess(exit) ? "succeeded" : failure === "interrupted" ? "interrupted" : "failed"
      const values = Exit.isSuccess(exit) ? metrics(exit.value) : Telemetry.empty
      const durationMs = Math.max(0, Number(end - began) / 1_000_000)
      // A tracer failure must not suppress logs. These are summary spans, not parents
      // of application work; raw causes must never be attached to them.
      yield* Effect.gen(function* () {
        if (span === null) return
        span.attribute("outcome", outcome)
        span.attribute("failure", failure)
        span.attribute("durationMs", durationMs)
        span.attribute("count", values.count)
        span.attribute("pageCount", values.pageCount)
        span.attribute("mode", values.mode)
        span.end(yield* Clock.currentTimeNanos, Exit.void)
      }).pipe(Effect.ignoreCause)
      yield* log(outcome, durationMs, failure, values)
    }).pipe(Effect.ignoreCause)
})

export const measure = <A, E, R>(
  operation: "Pull" | "Execute",
  effect: Effect.Effect<A, E, R>,
  page: "first" | "continuation" | null,
  metrics: (value: A) => Telemetry.Metrics
) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const finish = yield* start(operation, page).pipe(Effect.catchCause(() => Effect.succeed(null)))
      return yield* restore(effect).pipe(
        Effect.onExit((exit) => (finish === null ? Effect.void : finish(exit, metrics)))
      )
    })
  )

/** One pair per consumed connection, not per keepalive or presence frame. */
export const changes = <A, E, R>(stream: Stream.Stream<A, E, R>) =>
  Stream.unwrap(
    Effect.gen(function* () {
      yield* Effect.acquireRelease(
        start("Changes", null).pipe(Effect.catchCause(() => Effect.succeed(null))),
        (finish, exit) => (finish === null ? Effect.void : finish(exit, () => Telemetry.empty))
      )
      return stream
    })
  )
