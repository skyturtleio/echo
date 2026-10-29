import * as Domain from "echo/domain"
import { Story } from "echo/domain/testing"
import { acme, entryId, member, mondayMorning, otherMember, t } from "../fixtures.ts"
import { ProjectCreated } from "../Project/CreateProject.command.ts"
import { Code } from "../Project/Project.state.ts"
import { AmendEntry } from "./AmendEntry.command.ts"
import { Entry } from "./Entry.aggregate.ts"
import { EntryLogged, LogEntry, UnknownProject } from "./LogEntry.command.ts"
import { TimesOutOfOrder } from "./TimesOutOfOrder.rejection.ts"

const { privateScope, ScopeForbidden } = Domain.Scope

Story.make(Entry, "an amendment that ends an entry before it starts is refused on the merged row", {
  given: [
    new EntryLogged({ ...mondayMorning, startedAt: t("10:00"), endedAt: t("10:45"), scope: privateScope(member) })
  ],
  when: new AmendEntry({ target: entryId, changes: { endedAt: t("09:50") } }),
  as: member,
  then: new TimesOutOfOrder()
})

Story.make(Entry, "an entry against a project the member cannot see is refused", {
  given: {
    [`Project/${acme}`]: [
      new ProjectCreated({ code: Code.make("ACME"), name: "Acme Corp", scope: privateScope(otherMember) })
    ]
  },
  when: new LogEntry({ target: entryId, data: { ...mondayMorning, projectId: acme } }),
  as: member,
  then: new UnknownProject({ id: acme })
})

Story.make(Entry, "another member cannot amend my entry", {
  given: [new EntryLogged({ ...mondayMorning, scope: privateScope(member) })],
  when: new AmendEntry({ target: entryId, changes: { notes: "x" } }),
  as: otherMember,
  then: new ScopeForbidden({ scope: privateScope(member) })
})
