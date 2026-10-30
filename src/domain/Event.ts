import type * as Schema from "effect/Schema"
import { type Fields, taggedClass } from "./internal/classes.ts"

export type { Fields }

/** Instance type of an event class: its `_tag` plus its data. */
export type Instance<Tag extends string, F extends Fields> = { readonly _tag: Tag } & Schema.Struct.Type<F>

/** The class type returned by {@link make}. */
export interface EventClass<Tag extends string, F extends Fields> extends Schema.Class<
  Instance<Tag, F>,
  Schema.TaggedStruct<Tag, F>,
  {}
> {
  readonly kind: "Event"
  readonly tag: Tag
}

/** Structural constraint satisfied by every class produced by {@link make}. */
export interface Any extends Schema.Top {
  readonly kind: "Event"
  readonly tag: string
  readonly Type: { readonly _tag: string }
}

/**
 * A business-named fact recorded on one stream (`NoteWritten`, not `NoteUpdated`). Events
 * carry computed outcomes; `evolve` only applies them. Versioning (design §1) is not part of
 * Phase 0; an event class is its tag alone until it is.
 */
export const make = <const Tag extends string, const F extends Fields>(tag: Tag, fields: F): EventClass<Tag, F> =>
  Object.assign(taggedClass<Instance<Tag, F>, Tag, F>(tag, fields), { kind: "Event" as const, tag })
