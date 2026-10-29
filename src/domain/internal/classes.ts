import type * as Cause from "effect/Cause"
import * as Schema from "effect/Schema"

export type Fields = Schema.Struct.Fields

// `Schema.TaggedClass<Self>` and `Schema.TaggedError<Self>` give a type that is conditional on
// `Self` ("did the caller pass the generic?"). It resolves at a call site with a concrete `Self`,
// but inside a factory that is generic in `Self` it stays conditional, so it is neither a
// constructor to extend nor assignable to the class type. These two helpers are the one place
// the domain blocks cross that edge; everything above them is typed without assertions.

export const taggedClass = <Self, Tag extends string, F extends Fields>(
  tag: Tag,
  fields: F
): Schema.Class<Self, Schema.TaggedStruct<Tag, F>, {}> => {
  const made = Schema.TaggedClass<Self, {}>(tag)(tag, fields)
  // oxlint-disable-next-line typescript/consistent-type-assertions -- resolves the `Self` conditional, see above
  return made as Schema.Class<Self, Schema.TaggedStruct<Tag, F>, {}>
}

export const taggedError = <Self, Tag extends string, F extends Fields>(
  tag: Tag,
  fields: F
): Schema.Class<Self, Schema.TaggedStruct<Tag, F>, Cause.YieldableError> => {
  const made = Schema.TaggedError<Self, {}>(tag)(tag, fields)
  // oxlint-disable-next-line typescript/consistent-type-assertions -- resolves the `Self` conditional, see above
  return made as Schema.Class<Self, Schema.TaggedStruct<Tag, F>, Cause.YieldableError>
}

/**
 * A function type whose parameters are checked bivariantly, the way method parameters are.
 * `Aggregate.AnyDefinition` erases an aggregate's state and command types to `unknown`; a
 * handler written for `EntryState` must still be assignable to it so the story runner (and
 * later the engine) can call handlers of any aggregate. Only the erased side uses it: the typed
 * `Definition` keeps plain function types, so a handler cannot narrow the state it accepts.
 */
export type Bivariant<Args extends ReadonlyArray<unknown>, R> = {
  bivariant(...args: Args): R
}["bivariant"]
