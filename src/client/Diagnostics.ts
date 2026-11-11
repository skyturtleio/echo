import * as Arr from "effect/Array"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Telemetry from "../Telemetry.ts"
import { category, type Failure } from "../Telemetry.ts"

export type Operation = "execute" | "pull" | "journal" | "replica" | "rebase"
export type { Failure } from "../Telemetry.ts"
export interface DiagnosticOperation {
  readonly operation: Operation
  readonly durationMs: number
  readonly failure: Failure | null
  /** Receipts, scope entries, journal intents, persisted rows, or effective rows respectively. Null on failure. */
  readonly count: number | null
  /** Wire and IndexedDB storage bytes are not exposed accurately by these APIs. */
  readonly bytes: null
  readonly followsFailure: boolean
}
export interface Diagnostics {
  readonly operations: ReadonlyArray<DiagnosticOperation>
  /** Calls following a failed call of the same operation, not necessarily the same request or automatic. */
  readonly failedOperationFollowups: { readonly execute: number; readonly pull: number }
  readonly pending: { readonly count: number; readonly oldestAgeMs: number | null }
}

/** Private per-open recorder. Only allowlisted scalar fields cross the export boundary. */
export const make = (onTelemetry?: (event: Telemetry.TelemetryEvent) => void) => {
  let operations: ReadonlyArray<DiagnosticOperation> = []
  const failedOperationFollowups = { execute: 0, pull: 0 }
  const failed = { execute: false, pull: false }
  const measure = Effect.fnUntraced(function* <A, E, R>(
    operation: Operation,
    effect: Effect.Effect<A, E, R>,
    count: (value: A) => number
  ) {
    const request = operation === "execute" || operation === "pull" ? operation : null
    const followsFailure = request !== null && failed[request]
    if (followsFailure && request !== null) failedOperationFollowups[request]++
    const start = yield* Clock.monotonicTimeNanos.pipe(Effect.catchCause(() => Effect.succeed(null)))
    // Instrumentation must never prevent or retry the underlying operation.
    if (start === null) return yield* effect
    return yield* effect.pipe(
      Effect.onExit((exit) =>
        Effect.gen(function* () {
          const end = yield* Clock.monotonicTimeNanos
          const failure = Exit.isFailure(exit) ? category(exit.cause) : null
          if (request !== null) failed[request] = failure !== null
          operations = [
            ...operations.slice(-99),
            {
              operation,
              durationMs: Math.max(0, Number(end - start) / 1_000_000),
              failure,
              count: Exit.isSuccess(exit) ? count(exit.value) : null,
              bytes: null,
              followsFailure
            }
          ]
        }).pipe(Effect.ignoreCause)
      )
    )
  })
  return {
    measure: <A, E, R>(
      operation: Operation,
      effect: Effect.Effect<A, E, R>,
      count: (value: A) => number,
      metrics?: (value: A) => Telemetry.Metrics
    ) =>
      Telemetry.measure(
        operation,
        measure(operation, effect, count),
        onTelemetry,
        metrics ?? ((value) => ({ ...Telemetry.empty, count: count(value) }))
      ),
    snapshot: () => ({
      operations: Arr.map(operations, (entry) => ({ ...entry })),
      failedOperationFollowups: { ...failedOperationFollowups }
    })
  }
}
