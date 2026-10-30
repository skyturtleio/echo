import type * as DateTime from "effect/DateTime"
import * as Record from "effect/Record"
import type * as Schema from "effect/Schema"
import type * as Command from "./Command.ts"
import type * as Index from "./Indexes.ts"
import type { Bivariant } from "./internal/classes.ts"
import type { Principal, Scope } from "./Scope.ts"

/**
 * The instance type of a domain class: `InstanceOf<typeof TimesOutOfOrder>` is `TimesOutOfOrder`.
 * Read off the schema's `Type` rather than the constructor, because `Schema.Top` declares a
 * second construct signature that would win the inference. Distributes over a union.
 */
export type InstanceOf<C> = C extends { readonly Type: infer I extends { readonly _tag: string } } ? I : never

/** The members of a union of classes whose static `tag` is `T`. */
export type ByTag<U, T extends string> = Extract<U, { readonly tag: T }>

/** The event classes a list of commands may record. */
export type EventsOf<C extends ReadonlyArray<Command.Any>> = C[number]["events"][number]

/** What `decide` returns for a command: its declared events (empty for an accepted no-op) or one of its declared rejections. */
export type Decision<C extends Command.Any> =
  | ReadonlyArray<InstanceOf<C["events"][number]>>
  | InstanceOf<C["rejects"][number]>

/**
 * What `decide` sees besides its stream's state and the command (design §1). `read`
 * answers from the streams the principal may see: another stream by id (`null` when it does
 * not exist or is not visible) or every visible stream an index holds under a value.
 */
export interface DecideContext {
  readonly principal: Principal
  readonly now: DateTime.Utc
  read<A extends Any>(aggregate: A, id: IdOf<A>): StateOf<A> | null
  read<I extends Index.Any>(index: I, value: Index.Value<I>): ReadonlyArray<Index.Entry<I>>
}

/** The pure definition of an aggregate: identity, state, scope, indexes, the commands it decides and the events it folds. */
export interface Definition<
  Id extends Index.IdSchema,
  S extends Schema.Top,
  C extends ReadonlyArray<Command.Any>,
  X extends Index.Spec<S["Type"]>
> {
  /** Schema of the stream id. */
  readonly id: Id
  /** Schema of the state. */
  readonly state: S
  /** The state of a stream that has recorded no event. */
  readonly initial: S["Type"]
  /** Every command this aggregate handles, each declaring the events it records. */
  readonly commands: C
  /** Which scope the stream is visible in, from its state; `null` while it has no state (design §1). */
  readonly scope: (state: S["Type"]) => Scope | null
  /** The indexes built over this state, from {@link Index.make}. */
  readonly indexes: Index.Set<Id, S, X>
  /**
   * One pure handler per command. It sees only this stream's state, the command and `ctx`,
   * and returns the events to record or a rejection.
   */
  readonly decide: {
    readonly [T in C[number]["tag"]]: (
      state: S["Type"],
      command: InstanceOf<ByTag<C[number], T>>,
      ctx: DecideContext
    ) => Decision<ByTag<C[number], T>>
  }
  /** One pure, total handler per event: folds it into state. */
  readonly evolve: {
    readonly [T in EventsOf<C>["tag"]]: (state: S["Type"], event: InstanceOf<ByTag<EventsOf<C>, T>>) => S["Type"]
  }
}

/** A {@link Definition} with its types erased, as the story runner and the engine see one. */
export interface AnyDefinition {
  readonly id: Index.IdSchema
  readonly state: Schema.Top
  readonly initial: unknown
  readonly commands: ReadonlyArray<Command.Any>
  readonly scope: Bivariant<[state: unknown], Scope | null>
  readonly indexes: { readonly [name: string]: Index.Any }
  readonly decide: {
    readonly [tag: string]: Bivariant<[state: unknown, command: { readonly _tag: string }, ctx: DecideContext], unknown>
  }
  readonly evolve: { readonly [tag: string]: Bivariant<[state: unknown, event: { readonly _tag: string }], unknown> }
}

/** Structural constraint satisfied by every class produced by {@link make}. */
export interface Any {
  readonly kind: "Aggregate"
  readonly tag: string
  readonly definition: AnyDefinition
  new (): object
}

export type IdOf<A extends Any> = A["definition"]["id"]["Type"]
export type StateOf<A extends Any> = A["definition"]["state"]["Type"]
export type CommandsOf<A extends Any> = A["definition"]["commands"][number]
export type CommandInstance<A extends Any> = InstanceOf<CommandsOf<A>>
export type EventInstance<A extends Any> = InstanceOf<EventsOf<A["definition"]["commands"]>>

/** The class type returned by {@link make}: the definition plus one static per index (`Entry.by_task`). */
export type AggregateClass<
  Name extends string,
  Id extends Index.IdSchema,
  S extends Schema.Top,
  C extends ReadonlyArray<Command.Any>,
  X extends Index.Spec<S["Type"]>
> = {
  readonly kind: "Aggregate"
  readonly tag: Name
  readonly definition: Definition<Id, S, C, X>
  new (): object
} & Index.Set<Id, S, X>

const owners = new WeakMap<Index.Any, Any>()

/**
 * One kind of stream. A stream is `Name/id`; its state is `initial` folded through `evolve`
 * over its events; a command on it runs `decide` against that state (design §1).
 */
export const make = <
  const Name extends string,
  Id extends Index.IdSchema,
  S extends Schema.Top,
  const C extends ReadonlyArray<Command.Any>,
  const X extends Index.Spec<S["Type"]>
>(
  name: Name,
  definition: Definition<Id, S, C, X>
): AggregateClass<Name, Id, S, C, X> => {
  // oxlint-disable-next-line typescript/no-extraneous-class -- a class so an app writes `class Entry extends Domain.Aggregate(...)`, as with every other block
  class Aggregate {
    static readonly kind = "Aggregate" as const
    static readonly tag = name
    static readonly definition = definition
  }
  const indexes: { readonly [name: string]: Index.Any } = definition.indexes
  for (const index of Record.values(indexes)) {
    const owner = owners.get(index)
    if (owner !== undefined) {
      throw new Error(`index ${index.name} already belongs to ${owner.tag}; it cannot also belong to ${name}`)
    }
    owners.set(index, Aggregate)
  }
  return Object.assign(Aggregate, definition.indexes)
}

/** The aggregate an index was declared on. */
export const ofIndex = (index: Index.Any): Any => {
  const owner = owners.get(index)
  if (owner === undefined) throw new Error(`index ${index.name} is not declared on any aggregate`)
  return owner
}
