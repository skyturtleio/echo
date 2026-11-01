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

export const canWrite = (principal: Principal, scope: Scope, grants: ReadonlyArray<Grant> = []): boolean => {
  const permission = role(principal, scope, grants)
  return permission === "writer" || permission === "owner"
}

/**
 * The engine's answer when a principal commands a stream whose scope they hold no writer grant
 * on. It is checked once, generically, before `decide` runs, so no aggregate repeats the
 * ownership rule (design §2 item 8).
 */
export class ScopeForbidden extends Rejection.make("ScopeForbidden", { scope: Scope }) {}
