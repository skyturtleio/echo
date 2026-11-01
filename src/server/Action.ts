import type * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import type * as Aggregate from "../domain/Aggregate.ts"
import type { Grant, Principal, Scope } from "../domain/Scope.ts"

export interface Read {
  get<A extends Aggregate.Any>(aggregate: A, id: Aggregate.IdOf<A>): Aggregate.StateOf<A> | null
  scan<A extends Aggregate.Any>(
    aggregate: A
  ): ReadonlyArray<{ readonly id: Aggregate.IdOf<A>; readonly state: Aggregate.StateOf<A> }>
}

export type Operation =
  | { readonly kind: "command"; readonly aggregate: string; readonly command: unknown }
  | { readonly kind: "grant"; readonly grant: Grant }
  | { readonly kind: "revoke"; readonly scope: Scope; readonly principal: Principal }

export const command = <A extends Aggregate.Any>(aggregate: A, value: Aggregate.CommandInstance<A>): Operation => ({
  kind: "command",
  aggregate: aggregate.tag,
  command: value
})
export const grant = (value: Grant): Operation => ({ kind: "grant", grant: value })
export const revoke = (scope: Scope, principal: Principal): Operation => ({ kind: "revoke", scope, principal })

export interface Context {
  readonly principal: Principal
  readonly now: DateTime.Utc
  /** Privileged, limited to the action's declared aggregates. Action code must authorize its caller. */
  readonly read: Read
  readonly grants: ReadonlyArray<Grant>
}
export type Plan = { readonly operations: ReadonlyArray<Operation> } | { readonly rejection: unknown }
export interface Action {
  readonly name: string
  readonly input: Schema.Top
  readonly reads: ReadonlyArray<Aggregate.Any>
  readonly decide: (input: unknown, context: Context) => Plan
}

/** Trusted pure planner, re-run on serialization retry. Never perform external effects here. */
export const make = <S extends Schema.Top>(options: {
  readonly name: string
  readonly input: S
  readonly reads: ReadonlyArray<Aggregate.Any>
  readonly decide: (input: S["Type"], context: Context) => Plan
}): Action => ({
  ...options,
  decide: (input, context) => {
    // Platform boundary: actions, like domain definitions, require synchronous service-free codecs.
    const schema = Schema.make<Schema.Codec<S["Type"], unknown>>(Schema.toCodecJson(options.input).ast)
    // oxlint-disable-next-line effecttsgo/schema-sync
    return options.decide(Schema.decodeUnknownSync(schema)(input), context)
  }
})
