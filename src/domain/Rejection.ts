import type * as Cause from "effect/Cause"
import type * as Schema from "effect/Schema"
import { type Fields, taggedError } from "./internal/classes.ts"

/** Instance type of a rejection class: its `_tag` plus its evidence. */
export type Instance<Tag extends string, F extends Fields> = { readonly _tag: Tag } & Schema.Struct.Type<F>

/** The class type returned by {@link make}. */
export interface RejectionClass<Tag extends string, F extends Fields> extends Schema.Class<
  Instance<Tag, F>,
  Schema.TaggedStruct<Tag, F>,
  Cause.YieldableError
> {
  readonly kind: "Rejection"
  readonly tag: Tag
}

/** Structural constraint satisfied by every class produced by {@link make}. */
export interface Any extends Schema.Top {
  readonly kind: "Rejection"
  readonly tag: string
  readonly Type: { readonly _tag: string }
}

/**
 * The answer `decide` gives when a command is refused. A rejection is a typed error so the
 * engine can carry it through Effect; it records nothing on the stream.
 */
export const make = <const Tag extends string, const F extends Fields>(tag: Tag, fields: F): RejectionClass<Tag, F> =>
  Object.assign(taggedError<Instance<Tag, F>, Tag, F>(tag, fields), { kind: "Rejection" as const, tag })
