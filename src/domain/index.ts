/**
 * `echo/domain`: the building blocks an app's domain package is written against (design §1).
 * Every block is `Domain.<Noun>(...)` and its types live on the same name
 * (`Domain.Event.Any`, `Domain.Aggregate.StateOf<typeof Note>`), as in Alchemy.
 *
 * ```ts
 * import * as Domain from "echo/domain"
 *
 * export class TagRemoved extends Domain.Event("TagRemoved", {}) {}
 * ```
 *
 * `Policy`, `Batch`, command `authority` and versioning (design §1) arrive with the phases
 * that need them; no Phase 0 story did.
 */
import * as AggregateModule from "./Aggregate.ts"
import * as CommandModule from "./Command.ts"
import * as EventModule from "./Event.ts"
import * as IndexModule from "./Indexes.ts"
import * as RejectionModule from "./Rejection.ts"

export declare namespace Event {
  export type Any = EventModule.Any
  export type Instance<Tag extends string, F extends EventModule.Fields> = EventModule.Instance<Tag, F>
}
/** A business-named fact recorded on one stream. See {@link EventModule.make}. */
export const Event = EventModule.make

export declare namespace Rejection {
  export type Any = RejectionModule.Any
  export type Instance<Tag extends string, F extends EventModule.Fields> = RejectionModule.Instance<Tag, F>
}
/** The typed answer when a command is refused. See {@link RejectionModule.make}. */
export const Rejection = RejectionModule.make

export declare namespace Command {
  export type Any = CommandModule.Any
  export type Read = CommandModule.Read
  export type Instance<
    Tag extends string,
    Target extends IndexModule.IdSchema,
    F extends EventModule.Fields
  > = CommandModule.Instance<Tag, Target, F>
}
/** A request to change one stream. See {@link CommandModule.make}. */
export const Command = CommandModule.make

export declare namespace Index {
  export type Any = IndexModule.Any
  export type Value<I extends IndexModule.Any> = IndexModule.Value<I>
  export type Entry<I extends IndexModule.Any> = IndexModule.Entry<I>
}
/** The indexes built over an aggregate's state. See {@link IndexModule.make}. */
export const Index = IndexModule.make

export declare namespace Aggregate {
  export type Any = AggregateModule.Any
  export type Decision<C extends CommandModule.Any> = AggregateModule.Decision<C>
  export type DecideContext = AggregateModule.DecideContext
  export type IdOf<A extends AggregateModule.Any> = AggregateModule.IdOf<A>
  export type StateOf<A extends AggregateModule.Any> = AggregateModule.StateOf<A>
  export type InstanceOf<C> = AggregateModule.InstanceOf<C>
}
/** One kind of stream: state, scope, indexes, `decide` and `evolve`. See {@link AggregateModule.make}. */
export const Aggregate = AggregateModule.make

/** Principals, scopes, the Phase 0 grant rule and the engine's `ScopeForbidden` (design §1). */
export * as Scope from "./Scope.ts"

/** The pure story runner; `echo/domain/testing` binds it to vitest. */
export * as Story from "./Story.ts"
