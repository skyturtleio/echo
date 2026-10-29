import * as Domain from "echo/domain"
import { EntryId } from "./Entry.id.ts"
import { EntryChanges } from "./Entry.state.ts"
import { TimesOutOfOrder } from "./TimesOutOfOrder.rejection.ts"

export class EntryAmended extends Domain.Event("EntryAmended", { changes: EntryChanges }) {}

/** The entry was never logged, or has been deleted. */
export class NoSuchEntry extends Domain.Rejection("NoSuchEntry", {}) {}

export class AmendEntry extends Domain.Command("AmendEntry", {
  target: EntryId,
  input: { changes: EntryChanges },
  events: [EntryAmended],
  rejects: [TimesOutOfOrder, NoSuchEntry]
}) {}
