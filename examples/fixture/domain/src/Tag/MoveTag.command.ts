import * as Domain from "echo/domain"
import { TagId } from "./Tag.id.ts"
import { NoSuchTag } from "./RemoveTag.command.ts"

export class TagMoved extends Domain.Event("TagMoved", { scope: Domain.Scope.Scope }) {}
export class MoveTag extends Domain.Command("MoveTag", {
  target: TagId,
  input: { scope: Domain.Scope.Scope },
  events: [TagMoved],
  rejects: [NoSuchTag]
}) {}
export class RelocateTag extends Domain.Command("RelocateTag", {
  target: TagId,
  input: { scope: Domain.Scope.Scope },
  events: [TagMoved],
  rejects: [NoSuchTag],
  authority: "server"
}) {}
