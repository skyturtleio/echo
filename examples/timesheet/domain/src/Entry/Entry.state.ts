import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import * as Struct from "effect/Struct"
import { LocalTime } from "../LocalTime.ts"
import { ProjectId } from "../Project/Project.id.ts"
import { TaskId } from "../Task/Task.id.ts"

/** What a member writes in one timesheet entry. */
export const EntryData = Schema.Struct({
  /** ISO calendar date, `YYYY-MM-DD`, in the entry's local day. */
  day: Schema.String,
  projectId: ProjectId,
  taskId: TaskId,
  startedAt: LocalTime,
  endedAt: LocalTime,
  notes: Schema.String
})
export type EntryData = typeof EntryData.Type

/** A sparse patch of an entry: only the fields being changed, never `undefined`. */
export const EntryChanges = EntryData.mapFields(Struct.map(Schema.optionalKey))
export type EntryChanges = typeof EntryChanges.Type

/** A logged entry. `null` is the state of an entry stream nothing has happened to. */
export const EntryState = Schema.NullOr(
  Schema.Struct({
    ...EntryData.fields,
    /** The member's private scope; set when the entry is logged. */
    scope: Domain.Scope.Scope,
    deleted: Schema.Boolean
  })
)
export type EntryState = typeof EntryState.Type
