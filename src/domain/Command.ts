import type * as Schema from "effect/Schema"
import type * as Aggregate from "./Aggregate.ts"
import type * as Event from "./Event.ts"
import type * as Index from "./Indexes.ts"
import { type Fields, taggedClass } from "./internal/classes.ts"
import type * as Rejection from "./Rejection.ts"

/** The fields of a command instance: the id of the stream it addresses, then its input. */
export type InputFields<Target extends Index.IdSchema, F extends Fields> = { readonly target: Target } & F

/** Instance type of a command class: its `_tag`, its `target` and its input. */
export type Instance<Tag extends string, Target extends Index.IdSchema, F extends Fields> = {
  readonly _tag: Tag
} & Schema.Struct.Type<InputFields<Target, F>>

/**
 * What a command declares it reads besides its own stream: another aggregate by id, or an
 * index of any aggregate, including its own (design §2 item 2). The runner refuses a read
 * that is not declared, so the declaration is checked before any engine exists.
 */
export type Read = Aggregate.Any | Index.Any

/** The class type returned by {@link make}. */
export interface CommandClass<
  Tag extends string,
  Target extends Index.IdSchema,
  F extends Fields,
  Events extends ReadonlyArray<Event.Any>,
  Rejects extends ReadonlyArray<Rejection.Any>,
  Reads extends ReadonlyArray<Read>
> extends Schema.Class<Instance<Tag, Target, F>, Schema.TaggedStruct<Tag, InputFields<Target, F>>, {}> {
  readonly kind: "Command"
  readonly tag: Tag
  readonly target: Target
  readonly events: Events
  readonly rejects: Rejects | readonly []
  readonly reads: Reads | readonly []
  readonly authority: "both" | "server"
}

/** Structural constraint satisfied by every class produced by {@link make}. */
export interface Any extends Schema.Top {
  readonly kind: "Command"
  readonly tag: string
  readonly target: Index.IdSchema
  readonly events: ReadonlyArray<Event.Any>
  readonly rejects: ReadonlyArray<Rejection.Any>
  readonly reads: ReadonlyArray<Read>
  readonly authority: "both" | "server"
  readonly Type: { readonly _tag: string; readonly target: string }
}

/**
 * A request to change one stream. It names the stream (`target`), carries its input, and
 * declares the events it may record, the rejections it may return and the other streams its
 * `decide` reads. Server-authority commands run only inside registered trusted actions;
 * ordinary commands run on both device and server.
 */
export const make = <
  const Tag extends string,
  Target extends Index.IdSchema,
  const F extends Fields = {},
  const Events extends ReadonlyArray<Event.Any> = readonly [],
  const Rejects extends ReadonlyArray<Rejection.Any> = readonly [],
  const Reads extends ReadonlyArray<Read> = readonly []
>(
  tag: Tag,
  options: {
    readonly target: Target
    readonly input?: F
    readonly events: Events
    readonly rejects?: Rejects
    readonly reads?: Reads
    readonly authority?: "both" | "server"
  }
): CommandClass<Tag, Target, F, Events, Rejects, Reads> => {
  // `input` may be absent (then `F` is `{}`); `Object.assign` keeps the generic where a spread of an optional loses it.
  const fields: InputFields<Target, F> = Object.assign({ target: options.target }, options.input)
  return Object.assign(taggedClass<Instance<Tag, Target, F>, Tag, InputFields<Target, F>>(tag, fields), {
    kind: "Command" as const,
    tag,
    target: options.target,
    events: options.events,
    rejects: options.rejects ?? ([] as const),
    reads: options.reads ?? ([] as const),
    authority: options.authority ?? "both"
  })
}
