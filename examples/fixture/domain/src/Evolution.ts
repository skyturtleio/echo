import * as Domain from "echosync/domain"
import * as Schema from "effect/Schema"

// A historical pair meant exactly two units, even after the command starts accepting units.
export const TallyState = Schema.NullOr(Schema.Struct({ value: Schema.Int, scope: Domain.Scope.Scope }))
export class OldTallyAdded extends Domain.Event("TallyAdded", {
  pairs: Schema.Int,
  scope: Domain.Scope.Scope
}) {}
export class OldAddTally extends Domain.Command("AddTally", {
  target: Schema.String,
  input: { pairs: Schema.Int },
  events: [OldTallyAdded]
}) {}
export class TallyAdded extends Domain.Event(
  "TallyAdded",
  {
    units: Schema.Int,
    scope: Domain.Scope.Scope
  },
  {
    version: 2,
    unversioned: 1,
    history: [
      Domain.Payload.from(1, OldTallyAdded, "Each recorded pair was two units, not today's input unit", (old) => ({
        _tag: "TallyAdded" as const,
        units: old.pairs * 2,
        scope: old.scope
      }))
    ]
  }
) {}
export class AddTally extends Domain.Command("AddTally", {
  target: Schema.String,
  input: { units: Schema.Int },
  events: [TallyAdded],
  evolution: {
    version: 2,
    unversioned: 1,
    history: [
      Domain.Payload.from(1, OldAddTally, "An old request adds pairs; preserve its requested quantity", (old) => ({
        _tag: "AddTally" as const,
        target: old.target,
        units: old.pairs * 2
      }))
    ]
  }
}) {}
export class LegacyTally extends Domain.Aggregate("Tally", {
  id: Schema.String,
  state: TallyState,
  initial: null,
  commands: [OldAddTally],
  indexes: Domain.Index(Schema.String, TallyState, {}),
  scope: (state) => state?.scope ?? null,
  decide: {
    AddTally: (_, command, ctx) => [
      new OldTallyAdded({ pairs: command.pairs, scope: Domain.Scope.privateScope(ctx.principal) })
    ]
  },
  evolve: { TallyAdded: (state, event) => ({ value: (state?.value ?? 0) + event.pairs * 2, scope: event.scope }) }
}) {}
export class CurrentTally extends Domain.Aggregate("Tally", {
  id: Schema.String,
  state: TallyState,
  initial: null,
  commands: [AddTally],
  indexes: Domain.Index(Schema.String, TallyState, {}),
  scope: (state) => state?.scope ?? null,
  decide: {
    AddTally: (_, command, ctx) => [
      new TallyAdded({ units: command.units, scope: Domain.Scope.privateScope(ctx.principal) })
    ]
  },
  evolve: { TallyAdded: (state, event) => ({ value: (state?.value ?? 0) + event.units, scope: event.scope }) }
}) {}
export class ProjectedTally extends Domain.Aggregate("Tally", {
  ...CurrentTally.definition,
  state: Domain.Payload.projection(TallyState, 2),
  indexes: Domain.Index(Schema.String, TallyState, {})
}) {}
