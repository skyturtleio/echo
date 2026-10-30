import * as Arr from "effect/Array"
import * as Data from "effect/Data"
import * as DateTime from "effect/DateTime"
import * as Equal from "effect/Equal"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Record from "effect/Record"
import * as Str from "effect/String"
import * as Aggregate from "./Aggregate.ts"
import type * as Command from "./Command.ts"
import type * as Index from "./Indexes.ts"
import { canRead, canWrite, type Principal, ScopeForbidden } from "./Scope.ts"

/** Any event instance, for streams of aggregates other than the one under test. */
export interface SomeEvent {
  readonly _tag: string
}

/**
 * The events that already happened: a list for the stream the command targets, or streams by
 * key (`Entry/<id>`) when the rule needs other streams. A seeded stream must belong to the
 * aggregate under test or to one the command declares in `reads`.
 */
export type Given<A extends Aggregate.Any> =
  | ReadonlyArray<Aggregate.EventInstance<A>>
  | { readonly [stream: string]: ReadonlyArray<SomeEvent> }

/** The command class of the aggregate whose instances have this `_tag`. */
export type CommandClassOf<A extends Aggregate.Any, W extends SomeEvent> = Aggregate.ByTag<
  Aggregate.CommandsOf<A>,
  W["_tag"]
>

/** What the story expects: the command's declared events or rejections, or the engine's generic one. */
export type Then<A extends Aggregate.Any, W extends SomeEvent> =
  | Aggregate.Decision<CommandClassOf<A, W>>
  | ScopeForbidden

export interface Definition<A extends Aggregate.Any, W extends Aggregate.CommandInstance<A>> {
  readonly given: Given<A>
  readonly when: W
  readonly as: Principal
  readonly then: Then<A, W>
  /** `ctx.now` for the decision; a story that does not care gets a fixed instant. */
  readonly now?: DateTime.Utc
}

/** The story could not be run: it is malformed, not failed. */
export class StoryError extends Data.TaggedError("StoryError")<{ readonly message: string }> {}

interface Stream {
  readonly aggregate: Aggregate.Any
  readonly id: string
  readonly state: unknown
}

const fixedNow = DateTime.makeUnsafe("2026-01-01T00:00:00Z")

const key = (aggregate: Aggregate.Any, id: string): string => `${aggregate.tag}/${id}`

const describe = (read: Command.Read): string =>
  read.kind === "Aggregate" ? read.tag : `${Aggregate.ofIndex(read).tag}.${read.name}`

const isList = (given: Given<Aggregate.Any>): given is ReadonlyArray<SomeEvent> => Arr.isArray(given)

const fold = (aggregate: Aggregate.Any, events: ReadonlyArray<SomeEvent>): unknown =>
  Arr.reduce(events, aggregate.definition.initial, (state, event) => {
    const tag = event._tag
    const evolve = Record.get(aggregate.definition.evolve, tag)
    if (Option.isNone(evolve)) {
      throw new StoryError({ message: `${aggregate.tag} records no ${tag}; it cannot be given one` })
    }
    return evolve.value(state, event)
  })

/**
 * Runs one story (design §2 item 10): folds `given` through `evolve`, checks the principal may write
 * the target stream's scope, runs `decide` once with a `ctx` that reads only the seeded streams
 * the principal may see, and returns what `decide` (or the scope check) answered. Pure: no
 * clock, no engine, no database.
 */
export const run = <A extends Aggregate.Any, W extends Aggregate.CommandInstance<A>>(
  aggregate: A,
  story: Definition<A, W>
): unknown => {
  const when: { readonly _tag: string; readonly target: string } = story.when
  const tag = when._tag
  const principal = story.as

  const command = Arr.findFirst(aggregate.definition.commands, (c) => c.tag === tag)
  if (Option.isNone(command)) {
    throw new StoryError({ message: `${aggregate.tag} handles no ${tag}` })
  }
  const reads = command.value.reads
  const declared = (read: Command.Read): boolean => Arr.some(reads, (r) => r === read)

  const reachable = Record.fromEntries(
    Arr.map(
      [aggregate, ...Arr.map(reads, (read) => (read.kind === "Aggregate" ? read : Aggregate.ofIndex(read)))],
      (a): [string, Aggregate.Any] => [a.tag, a]
    )
  )

  // Widened to the erased aggregate so the list-or-record shape can be told apart.
  const seeded: Given<Aggregate.Any> = story.given
  const given: { readonly [stream: string]: ReadonlyArray<SomeEvent> } = isList(seeded)
    ? { [key(aggregate, when.target)]: seeded }
    : seeded

  const streams = Record.map(given, (events, stream): Stream => {
    const slash = Str.indexOf("/")(stream)
    const name = Option.isSome(slash) ? Str.slice(0, slash.value)(stream) : stream
    const owner = Record.get(reachable, name)
    if (Option.isNone(slash) || Option.isNone(owner)) {
      throw new StoryError({
        message: `given seeds ${stream}, but ${tag} on ${aggregate.tag} reaches no aggregate ${name}; declare it in reads`
      })
    }
    return { aggregate: owner.value, id: Str.slice(slash.value + 1)(stream), state: fold(owner.value, events) }
  })

  const visible = (stream: Stream): boolean => {
    const scope = stream.aggregate.definition.scope(stream.state)
    return scope !== null && canRead(principal, scope)
  }

  const target = Record.get(streams, key(aggregate, when.target))
  const state = Option.isSome(target) ? target.value.state : aggregate.definition.initial
  if (Option.isSome(target)) {
    const scope = aggregate.definition.scope(state)
    if (scope !== null && !canWrite(principal, scope)) return new ScopeForbidden({ scope })
  }

  function read<R extends Aggregate.Any>(ref: R, id: Aggregate.IdOf<R>): Aggregate.StateOf<R> | null
  function read<I extends Index.Any>(ref: I, value: Index.Value<I>): ReadonlyArray<Index.Entry<I>>
  function read(ref: Command.Read, arg: unknown): unknown {
    if (!declared(ref)) {
      throw new StoryError({ message: `${tag} reads ${describe(ref)} without declaring it in reads` })
    }
    if (ref.kind === "Aggregate") {
      if (!Predicate.isString(arg)) throw new StoryError({ message: `a read of ${ref.tag} takes an id` })
      const stream = Record.get(streams, key(ref, arg))
      return Option.isSome(stream) && visible(stream.value) ? stream.value.state : null
    }
    const owner = Aggregate.ofIndex(ref)
    const values: unknown = ref.fields.length === 1 ? [arg] : arg
    if (!Arr.isArray(values)) {
      throw new StoryError({ message: `a read over ${describe(ref)} takes one value per field, as a tuple` })
    }
    const matches = (stream: Stream): boolean =>
      Arr.every(
        ref.fields,
        (field, i) => Predicate.hasProperty(stream.state, field) && Equal.equals(stream.state[field], values[i])
      )
    return Arr.map(
      Arr.filter(Record.values(streams), (stream) => stream.aggregate === owner && visible(stream) && matches(stream)),
      (stream) => ({ id: stream.id, state: stream.state })
    )
  }

  const ctx: Aggregate.DecideContext = { principal, now: story.now ?? fixedNow, read }
  const decide = Record.get(aggregate.definition.decide, tag)
  if (Option.isNone(decide)) {
    throw new StoryError({ message: `${aggregate.tag} declares ${tag} but has no decide handler for it` })
  }
  return decide.value(state, when, ctx)
}
