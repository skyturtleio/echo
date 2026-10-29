import * as Domain from "echo/domain"
import { NoSuchEntry } from "./AmendEntry.command.ts"
import { EntryId } from "./Entry.id.ts"

export class EntryDeleted extends Domain.Event("EntryDeleted", {}) {}

export class DeleteEntry extends Domain.Command("DeleteEntry", {
  target: EntryId,
  events: [EntryDeleted],
  rejects: [NoSuchEntry]
}) {}
