import * as Record from "effect/Record"
import type * as Schema from "effect/Schema"

/** The schema of an aggregate id: a string, usually branded. */
export type IdSchema = Schema.Top & { readonly Type: string; readonly Encoded: string }

/** The string keys of any member of a (possibly union) state type. */
export type KeysOf<S> = S extends unknown ? keyof S & string : never

/** The type of field `K` in whichever members of `S` carry it. */
export type FieldType<S, K extends string> = S extends { readonly [P in K]: infer V } ? V : never

/** The members of `S` that carry every one of the fields `K`, i.e. the states an index holds. */
export type Indexed<S, K extends string> = S extends { readonly [P in K]: unknown } ? S : never

/** Index names to the state fields they are built over, as written in an aggregate. */
export type Spec<S> = { readonly [name: string]: ReadonlyArray<KeysOf<S>> }

/**
 * One index over an aggregate's state: the client replica and the server both build it over
 * the named state fields, queries use it, and `decide` reads other streams through it
 * (design §1, §2 item 2).
 */
export interface Index<Id extends IdSchema, S extends Schema.Top, Fields extends ReadonlyArray<string>> {
  readonly kind: "Index"
  readonly name: string
  readonly id: Id
  readonly state: S
  readonly fields: Fields
}

export type Any = Index<IdSchema, Schema.Top, ReadonlyArray<string>>

export type Set<Id extends IdSchema, S extends Schema.Top, X extends Spec<S["Type"]>> = {
  readonly [K in keyof X]: Index<Id, S, X[K]>
}

/** The value a read over the index takes: the field's value, or a tuple when the index has several. */
export type Value<I extends Any> =
  I extends Index<IdSchema, infer S, infer Fields>
    ? Fields extends readonly [infer K extends string]
      ? FieldType<S["Type"], K>
      : { readonly [P in keyof Fields]: FieldType<S["Type"], Fields[P] & string> }
    : never

/** What a read over the index returns for each matching stream. */
export type Entry<I extends Any> =
  I extends Index<infer Id, infer S, infer Fields>
    ? { readonly id: Id["Type"]; readonly state: Indexed<S["Type"], Fields[number]> }
    : never

/**
 * Declares an aggregate's indexes. They are declared apart from the aggregate so a command can
 * name one of its own aggregate's indexes in `reads` without importing the aggregate (which
 * imports the command). `Aggregate.make` binds them to the aggregate.
 */
export const make = <Id extends IdSchema, S extends Schema.Top, const X extends Spec<S["Type"]>>(
  id: Id,
  state: S,
  spec: X
): Set<Id, S, X> => {
  const set = Record.map(spec, (fields, name): Any => ({ kind: "Index", name: String(name), id, state, fields }))
  // oxlint-disable-next-line typescript/consistent-type-assertions -- `Record.map` cannot express a per-key mapped type
  return set as Set<Id, S, X>
}
