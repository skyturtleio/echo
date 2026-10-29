import * as Domain from "echo/domain"
import { Project } from "../Project/Project.aggregate.ts"
import { ProjectId } from "../Project/Project.id.ts"
import { EntryId } from "./Entry.id.ts"
import { EntryData } from "./Entry.state.ts"
import { TimesOutOfOrder } from "./TimesOutOfOrder.rejection.ts"

export class EntryLogged extends Domain.Event("EntryLogged", {
  ...EntryData.fields,
  scope: Domain.Scope.Scope
}) {}

/** No project with this id is visible to the member: it does not exist, or it is another member's. */
export class UnknownProject extends Domain.Rejection("UnknownProject", { id: ProjectId }) {}

export class EntryAlreadyLogged extends Domain.Rejection("EntryAlreadyLogged", {}) {}

export class LogEntry extends Domain.Command("LogEntry", {
  target: EntryId,
  input: { data: EntryData },
  events: [EntryLogged],
  rejects: [UnknownProject, TimesOutOfOrder, EntryAlreadyLogged],
  reads: [Project]
}) {}
