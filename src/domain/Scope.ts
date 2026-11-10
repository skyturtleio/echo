import * as Arr from "effect/Array"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Str from "effect/String"
import * as Rejection from "./Rejection.ts"

/** Who sent a command: the authenticated user id (design §1). */
export const Principal = Schema.String.pipe(Schema.brand("Principal"))
export type Principal = typeof Principal.Type

/**
 * An opaque visibility id (`user:<id>`, `shared:<id>`, `catalog:projects`). Every stream is
 * in exactly one scope at a time, given by `Aggregate.scope(state)` (design §1).
 */
export const Scope = Schema.String.pipe(Schema.brand("Scope"))
export type Scope = typeof Scope.Type

/** The scope only its principal can see and write. */
export const privateScope = (principal: Principal): Scope => Scope.make(`user:${principal}`)

/** A catalog scope: every principal reads it, only the server writes it (design §1). */
export const catalogScope = (name: string): Scope => Scope.make(`catalog:${name}`)

export const Role = Schema.Literals(["reader", "writer", "owner"])
export type Role = typeof Role.Type
export const Grant = Schema.Struct({ scope: Scope, principal: Principal, role: Role })
export type Grant = typeof Grant.Type

/** Private ownership and public catalog reads are intrinsic, never transferable by grants. */
export const role = (principal: Principal, scope: Scope, grants: ReadonlyArray<Grant> = []): Role | null => {
  if (scope === privateScope(principal)) return "owner"
  if (Str.startsWith("catalog:")(scope)) return "reader"
  if (Str.startsWith("user:")(scope)) return null
  return (
    Option.getOrUndefined(Arr.findFirst(grants, (grant) => grant.scope === scope && grant.principal === principal))
      ?.role ?? null
  )
}

export const canRead = (principal: Principal, scope: Scope, grants: ReadonlyArray<Grant> = []): boolean =>
  role(principal, scope, grants) !== null

/** Minimum role for a command; reader access never permits mutation. */
export type WriteAccess = "writer" | "owner"

export const canWrite = (
  principal: Principal,
  scope: Scope,
  grants: ReadonlyArray<Grant> = [],
  access: WriteAccess = "writer"
): boolean => {
  const permission = role(principal, scope, grants)
  return permission === "owner" || (access === "writer" && permission === "writer")
}

/**
 * The engine's answer when a principal lacks a command's required scope role. Checked before
 * decide and against the resulting scope, so aggregates do not repeat role gates.
 */
export class ScopeForbidden extends Rejection.make("ScopeForbidden", { scope: Scope }) {}
