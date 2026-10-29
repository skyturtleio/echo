import * as Domain from "echo/domain"
import * as Arr from "effect/Array"
import { Entry } from "../Entry/Entry.aggregate.ts"
import { AddTask, TaskAdded, TaskAlreadyAdded } from "./AddTask.command.ts"
import { TaskId } from "./Task.id.ts"
import { TaskState } from "./Task.state.ts"
import { TaskInUse, TaskRemoved, NoSuchTask, RemoveTask } from "./RemoveTask.command.ts"

/** One stream per task a member books time on, in that member's private scope. */
export class Task extends Domain.Aggregate("Task", {
  id: TaskId,
  state: TaskState,
  initial: null,
  commands: [AddTask, RemoveTask],
  scope: (state) => state?.scope ?? null,
  indexes: Domain.Index(TaskId, TaskState, { by_project: ["projectId"] }),
  decide: {
    AddTask: (state, { data }, ctx) =>
      state !== null
        ? new TaskAlreadyAdded()
        : [new TaskAdded({ ...data, scope: Domain.Scope.privateScope(ctx.principal) })],
    RemoveTask: (state, { target }, ctx) => {
      if (state === null || state.removed) return new NoSuchTask()
      const entries = Arr.filter(ctx.read(Entry.by_task, target), (entry) => !entry.state.deleted).length
      return entries > 0 ? new TaskInUse({ entries }) : [new TaskRemoved()]
    }
  },
  evolve: {
    TaskAdded: (_, { name, projectId, scope }) => ({
      name,
      projectId,
      scope,
      removed: false
    }),
    TaskRemoved: (state) => (state === null ? state : { ...state, removed: true })
  }
}) {}
