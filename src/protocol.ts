import * as Schema from "effect/Schema"
import { Rpc, RpcGroup } from "effect/rpc"
import { Grant } from "./domain/Scope.ts"

export const StreamRef = Schema.Struct({ aggregate: Schema.String, id: Schema.String })
export const Position = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)
export const StreamSeq = Schema.Struct({ ...StreamRef.fields, seq: Position })
export const Batch = Schema.Struct({
  id: Schema.String,
  commands: Schema.Array(Schema.Struct({ aggregate: Schema.String, command: Schema.Unknown })),
  action: Schema.optional(Schema.Struct({ name: Schema.String, input: Schema.Unknown }))
})
export type Batch = typeof Batch.Type
export const Receipt = Schema.Union([
  Schema.TaggedStruct("Accepted", { batchId: Schema.String, revision: Position, seqs: Schema.Array(StreamSeq) }),
  Schema.TaggedStruct("Rejected", { batchId: Schema.String, rejection: Schema.Unknown })
])
export type Receipt = typeof Receipt.Type
export const Entry = Schema.Union([
  Schema.TaggedStruct("Upsert", { ...StreamSeq.fields, state: Schema.Unknown }),
  Schema.TaggedStruct("Gone", StreamRef.fields)
])
export const ExecuteRequest = Schema.Struct({
  epoch: Schema.NullOr(Schema.String),
  clientId: Schema.String,
  batches: Schema.Array(Batch)
})
export type ExecuteRequest = typeof ExecuteRequest.Type
export const ExecuteResponse = Schema.Struct({ epoch: Schema.String, receipts: Schema.Array(Receipt) })
export type ExecuteResponse = typeof ExecuteResponse.Type
export const PullRequest = Schema.Struct({
  epoch: Schema.NullOr(Schema.String),
  cursors: Schema.Record(Schema.String, Position)
})
export type PullRequest = typeof PullRequest.Type
/**
 * Scopes are the complete authorized membership list; evict cached scopes absent here.
 * reset replaces that scope's entire replica (including an empty snapshot), atomically
 * with its cursor. An unchanged cursor has reset=false and no entries. Execute receipts
 * never advance these cursors; retain accepted intents until replica sequences cover them.
 */
export const PullResponse = Schema.Struct({
  epoch: Schema.String,
  revision: Position,
  grants: Schema.Array(Grant),
  scopes: Schema.Array(
    Schema.Struct({
      scope: Schema.String,
      cursor: Position,
      reset: Schema.Boolean,
      entries: Schema.Array(Entry)
    })
  )
})
export type PullResponse = typeof PullResponse.Type
export const PresenceMember = Schema.Struct({
  room: Schema.String,
  principal: Schema.String,
  clientId: Schema.String,
  state: Schema.Json
})
export type PresenceMember = typeof PresenceMember.Type
export const PresenceRequest = Schema.Struct({
  connectionId: Schema.String,
  room: Schema.String,
  clientId: Schema.String,
  state: Schema.NullOr(Schema.Json)
})
export type PresenceRequest = typeof PresenceRequest.Type
export const Change = Schema.Struct({
  _tag: Schema.Literals(["Poke", "Keepalive"]),
  epoch: Schema.String,
  connectionId: Schema.optional(Schema.String),
  presence: Schema.optional(Schema.Array(PresenceMember))
})
export type Change = typeof Change.Type
export class ResyncRequired extends Schema.TaggedError<ResyncRequired, { readonly brand: unique symbol }>()(
  "ResyncRequired",
  { epoch: Schema.String }
) {}
export class ServerError extends Schema.TaggedError<ServerError, { readonly brand: unique symbol }>()("ServerError", {
  message: Schema.String
}) {}
export const EchoRpcs = RpcGroup.make(
  Rpc.make("Execute", {
    payload: ExecuteRequest,
    success: ExecuteResponse,
    error: Schema.Union([ResyncRequired, ServerError])
  }),
  Rpc.make("Pull", { payload: PullRequest, success: PullResponse, error: Schema.Union([ResyncRequired, ServerError]) }),
  Rpc.make("Changes", { payload: {}, success: Change, error: ServerError, stream: true }),
  Rpc.make("Presence", {
    payload: PresenceRequest,
    success: Schema.Void,
    error: Schema.Union([ResyncRequired, ServerError])
  })
)
