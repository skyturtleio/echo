import type * as Schema from "effect/Schema"
import { type Codec, type Fields, taggedClass } from "./internal/classes.ts"
import * as Payload from "./Payload.ts"

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
  readonly evolution?: Payload.Evolution<Instance<Tag, F>>
}

/** Structural constraint satisfied by every class produced by {@link make}. */
export interface Any extends Codec {
  readonly kind: "Event"
  readonly tag: string
  readonly Type: { readonly _tag: string }
  readonly evolution?: Payload.Evolution<unknown>
}

/**
 * A business-named fact recorded on one stream (`NoteWritten`, not `NoteUpdated`). Events
 * carry computed outcomes; `evolve` only applies them. Historical adapters preserve facts,
 * not today's command decisions. Omitting evolution retains the original unversioned v1 format.
 */
export const make = <const Tag extends string, const F extends Fields>(
  tag: Tag,
  fields: F,
  evolution?: Payload.Evolution<Instance<Tag, F>>
): EventClass<Tag, F> => {
  if ("_version" in fields) throw new Error("_version is reserved for persisted payloads")
  if (evolution !== undefined) Payload.inspect(evolution)
  return Object.assign(taggedClass<Instance<Tag, F>, Tag, F>(tag, fields), {
    kind: "Event" as const,
    tag,
    ...(evolution === undefined ? {} : { evolution })
  })
}
