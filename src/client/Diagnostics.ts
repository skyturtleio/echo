import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

export type Operation = "execute" | "pull" | "journal" | "replica" | "rebase"
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

const category = (cause: Cause.Cause<unknown>): Failure => {
  if (Cause.hasInterruptsOnly(cause)) return "interrupted"
  const error = Option.getOrUndefined(Cause.findErrorOption(cause))
  if (
    Schema.is(
      Schema.TaggedStruct("TransportError", {
        reason: Schema.Literals(["offline", "auth", "oversized"])
      })
    )(error)
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

/** Private per-open recorder. Only allowlisted scalar fields cross the export boundary. */
export const make = () => {
  let operations: ReadonlyArray<DiagnosticOperation> = []
  const failedOperationFollowups = { execute: 0, pull: 0 }
  const failed = { execute: false, pull: false }
  const measure = <A, E, R>(operation: Operation, effect: Effect.Effect<A, E, R>, count: (value: A) => number) =>
    Effect.gen(function* () {
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
    measure,
    snapshot: () => ({
      operations: Arr.map(operations, (entry) => ({ ...entry })),
      failedOperationFollowups: { ...failedOperationFollowups }
    })
  }
}
