import * as Domain from "echo/domain"
import { Project } from "../Project/Project.aggregate.ts"
import { inOrder } from "../LocalTime.ts"
import { AmendEntry, EntryAmended, NoSuchEntry } from "./AmendEntry.command.ts"
import { DeleteEntry, EntryDeleted } from "./DeleteEntry.command.ts"
import { EntryId } from "./Entry.id.ts"
import { EntryIndexes } from "./Entry.indexes.ts"
import { EntryState } from "./Entry.state.ts"
import { EntryAlreadyLogged, EntryLogged, LogEntry, UnknownProject } from "./LogEntry.command.ts"
import { TimesOutOfOrder } from "./TimesOutOfOrder.rejection.ts"

/**
 * One stream per timesheet entry, in its member's private scope. Who may amend or delete it is
 * not decided here: the engine refuses a command on a stream whose scope the principal cannot
 * write before `decide` runs (design §2 item 8).
 */
export class Entry extends Domain.Aggregate("Entry", {
  id: EntryId,
  state: EntryState,
  initial: null,
  commands: [LogEntry, AmendEntry, DeleteEntry],
  scope: (state) => state?.scope ?? null,
  indexes: EntryIndexes,
  decide: {
    LogEntry: (state, { data }, ctx) => {
      if (state !== null) return new EntryAlreadyLogged()
      const project = ctx.read(Project, data.projectId)
      if (project === null) return new UnknownProject({ id: data.projectId })
      if (!inOrder(data)) return new TimesOutOfOrder()
      return [new EntryLogged({ ...data, scope: Domain.Scope.privateScope(ctx.principal) })]
    },
    AmendEntry: (state, { changes }) => {
      if (state === null || state.deleted) return new NoSuchEntry()
      // A sparse patch: the rule is checked on the merged row, not on the patch.
      if (!inOrder({ ...state, ...changes })) return new TimesOutOfOrder()
      return [new EntryAmended({ changes })]
    },
    DeleteEntry: (state) => (state === null || state.deleted ? new NoSuchEntry() : [new EntryDeleted()])
  },
  evolve: {
    EntryLogged: (_, event) => ({
      day: event.day,
      projectId: event.projectId,
      taskId: event.taskId,
      startedAt: event.startedAt,
      endedAt: event.endedAt,
      notes: event.notes,
      scope: event.scope,
      deleted: false
    }),
    EntryAmended: (state, { changes }) => (state === null ? state : { ...state, ...changes }),
    EntryDeleted: (state) => (state === null ? state : { ...state, deleted: true })
  }
}) {}
