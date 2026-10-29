import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import * as Str from "effect/String"

/** A project code as stored: trimmed and upper-cased, so "acme" and "ACME" are one project. */
export const Code = Schema.String.pipe(Schema.brand("Code"))
export type Code = typeof Code.Type

export const normaliseCode = (raw: string): Code => Code.make(Str.toUpperCase(Str.trim(raw)))

/** A created project. `null` is the state of a project stream nothing has happened to. */
export const ProjectState = Schema.NullOr(
  Schema.Struct({
    code: Code,
    name: Schema.String,
    /** The member's private scope, or `catalog:projects` for the shared catalog the server maintains. */
    scope: Domain.Scope.Scope
  })
)
export type ProjectState = typeof ProjectState.Type
