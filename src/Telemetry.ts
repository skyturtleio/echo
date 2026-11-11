import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

// oxlint-disable-next-line effecttsgo/crypto-random-uuid-in-effect -- operation identity uses the host cryptographic source without requiring application services
export const randomId = Effect.sync(() => globalThis.crypto.randomUUID())

export type Failure =
  | "offline"
  | "auth"
  | "oversized"
  | "transport"
  | "storage"
  | "resync"
  | "client"
  | "interrupted"
  | "unknown"
export interface Metrics {
  readonly count: number | null
  readonly pageCount: number | null
  readonly mode: "bootstrap" | "incremental" | "mixed" | null
}
export interface TelemetryEvent extends Metrics {
  readonly operationId: string
  readonly operation: "open" | "sync" | "pull" | "execute" | "journal" | "replica" | "rebase"
  readonly outcome: "started" | "succeeded" | "failed" | "interrupted" | "stalled"
  readonly durationMs: number
  readonly failure: Failure | null
}
export const category = (cause: Cause.Cause<unknown>): Failure => {
  if (Cause.hasInterruptsOnly(cause)) return "interrupted"
  const error = Option.getOrUndefined(Cause.findErrorOption(cause))
  if (
    Schema.is(Schema.TaggedStruct("TransportError", { reason: Schema.Literals(["offline", "auth", "oversized"]) }))(
      error
    )
  )
    return error.reason
  if (Schema.is(Schema.Struct({ _tag: Schema.String }))(error)) {
    switch (error._tag) {
      case "TransportError":
        return "transport"
      case "StorageError":
        return "storage"
      case "ResyncRequired":
        return "resync"
      case "ClientError":
        return "client"
    }
  }
  return "unknown"
}
export const empty: Metrics = { count: null, pageCount: null, mode: null }
export const pageMetrics = (reply: {
  readonly scopes: ReadonlyArray<{ readonly reset: boolean; readonly entries: ReadonlyArray<unknown> }>
}): Metrics => ({
  count: Arr.reduce(reply.scopes, 0, (n, scope) => n + scope.entries.length),
  pageCount: 1,
  mode: Arr.some(reply.scopes, (scope) => scope.reset)
    ? Arr.some(reply.scopes, (scope) => !scope.reset)
      ? "mixed"
      : "bootstrap"
    : "incremental"
})

/** The observer receives fresh allowlisted scalars, never the operation result or cause. */
export const measure = <A, E, R>(
  operation: TelemetryEvent["operation"],
  effect: Effect.Effect<A, E, R>,
  observer: ((event: TelemetryEvent) => void) | undefined,
  metrics: (value: A) => Metrics = () => empty
): Effect.Effect<A, E, R> => {
  if (observer === undefined) return effect
  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const setup = yield* Effect.gen(function* () {
        return { operationId: yield* randomId, start: yield* Clock.monotonicTimeNanos }
      }).pipe(Effect.catchCause(() => Effect.succeed(null)))
      if (setup === null) return yield* restore(effect)
      const emit = Effect.fnUntraced(function* (
        outcome: TelemetryEvent["outcome"],
        failure: Failure | null,
        values: Metrics = empty
      ) {
        const end = yield* Clock.monotonicTimeNanos
        observer({
          operationId: setup.operationId,
          operation,
          outcome,
          durationMs: Math.max(0, Number(end - setup.start) / 1_000_000),
          failure,
          ...values
        })
      }, Effect.ignoreCause())
      yield* emit("started", null)
      const stalled = yield* Effect.sleep("10 seconds").pipe(Effect.andThen(emit("stalled", null)), Effect.forkChild)
      return yield* restore(effect).pipe(
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            yield* Fiber.interrupt(stalled)
            const failure = Exit.isFailure(exit) ? category(exit.cause) : null
            yield* Effect.suspend(() =>
              emit(
                Exit.isSuccess(exit) ? "succeeded" : failure === "interrupted" ? "interrupted" : "failed",
                failure,
                Exit.isSuccess(exit) ? metrics(exit.value) : empty
              )
            ).pipe(Effect.ignoreCause)
          })
        )
      )
    })
  )
}
