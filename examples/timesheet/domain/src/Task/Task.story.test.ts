import * as Domain from "echo/domain"
import { Story } from "echo/domain/testing"
import { Entry } from "../Entry/Entry.aggregate.ts"
import { EntryLogged } from "../Entry/LogEntry.command.ts"
import { acme, entryId, member, mondayMorning, review } from "../fixtures.ts"
import { TaskAdded } from "./AddTask.command.ts"
import { RemoveTask, TaskInUse } from "./RemoveTask.command.ts"
import { Task } from "./Task.aggregate.ts"

const { privateScope } = Domain.Scope

Story.make(Task, "a task with entries cannot be removed", {
  given: [
    Story.stream(Task, review, [new TaskAdded({ name: "Code review", projectId: acme, scope: privateScope(member) })]),
    Story.stream(Entry, entryId, [new EntryLogged({ ...mondayMorning, taskId: review, scope: privateScope(member) })])
  ],
  when: new RemoveTask({ target: review }),
  as: member,
  then: new TaskInUse({ entries: 1 })
})
