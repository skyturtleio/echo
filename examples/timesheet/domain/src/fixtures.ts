/** Shared story fixtures: two members, their ids, and one Monday-morning entry. */
import * as Domain from "echo/domain"
import type { EntryData } from "./Entry/Entry.state.ts"
import { EntryId } from "./Entry/Entry.id.ts"
import { LocalTime } from "./LocalTime.ts"
import { ProjectId } from "./Project/Project.id.ts"
import { TaskId } from "./Task/Task.id.ts"

export const member = Domain.Scope.Principal.make("member-ada")
export const otherMember = Domain.Scope.Principal.make("member-grace")

export const entryId = EntryId.make("entry-1")
export const acme = ProjectId.make("project-acme")
export const acmeAgain = ProjectId.make("project-acme-again")
export const review = TaskId.make("task-review")

export const t = (hhmm: string): LocalTime => LocalTime.make(hhmm)

export const mondayMorning: EntryData = {
  day: "2026-03-16",
  projectId: acme,
  taskId: review,
  startedAt: t("09:00"),
  endedAt: t("10:30"),
  notes: ""
}
