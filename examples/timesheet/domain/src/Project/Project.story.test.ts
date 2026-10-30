import * as Domain from "echo/domain"
import { Story } from "echo/domain/testing"
import { acme, acmeAgain, member } from "../fixtures.ts"
import { CreateProject, DuplicateCode, ProjectCreated } from "./CreateProject.command.ts"
import { Project } from "./Project.aggregate.ts"
import { Code } from "./Project.state.ts"

const { privateScope } = Domain.Scope

Story.make(Project, "a second project with the same code is refused", {
  given: [
    Story.stream(Project, acme, [
      new ProjectCreated({ code: Code.make("ACME"), name: "Acme Corp", scope: privateScope(member) })
    ])
  ],
  when: new CreateProject({ target: acmeAgain, data: { code: " acme ", name: "Acme Corporation" } }),
  as: member,
  then: new DuplicateCode({ existing: acme })
})
