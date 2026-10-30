import * as Domain from "echo/domain"
import * as Arr from "effect/Array"
import * as Option from "effect/Option"
import { ProjectId } from "./Project.id.ts"
import { ProjectIndexes } from "./Project.indexes.ts"
import { ProjectState, normaliseCode } from "./Project.state.ts"
import { CreateProject, DuplicateCode, ProjectAlreadyCreated, ProjectCreated } from "./CreateProject.command.ts"

/**
 * One stream per project. A member's own projects live in their private scope; the shared
 * catalog (`catalog:projects`) is written by the server only (design §1).
 */
export class Project extends Domain.Aggregate("Project", {
  id: ProjectId,
  state: ProjectState,
  initial: null,
  commands: [CreateProject],
  scope: (state) => state?.scope ?? null,
  indexes: ProjectIndexes,
  decide: {
    CreateProject: (state, { data }, ctx) => {
      if (state !== null) return new ProjectAlreadyCreated()
      const code = normaliseCode(data.code)
      const existing = Arr.head(ctx.read(ProjectIndexes.by_code, code))
      if (Option.isSome(existing)) return new DuplicateCode({ existing: existing.value.id })
      return [new ProjectCreated({ code, name: data.name, scope: Domain.Scope.privateScope(ctx.principal) })]
    }
  },
  evolve: {
    ProjectCreated: (_, { code, name, scope }) => ({ code, name, scope })
  }
}) {}
