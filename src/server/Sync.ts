import * as PgClient from "@effect/sql-pg/PgClient"
import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { SqlClient } from "effect/sql"
import { Grant, privateScope, type Principal } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"

/** Called only by the explicit engine migration. Existing cursors become the log floor. */
export const migrate = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* sql`UPDATE echosync_engine SET cursor=cursor`
  yield* sql`CREATE TABLE IF NOT EXISTS echosync_scope_floors(scope text PRIMARY KEY, cursor bigint NOT NULL)`
  yield* sql`INSERT INTO echosync_scope_floors SELECT scope,cursor FROM echosync_scopes ON CONFLICT DO NOTHING`
  yield* sql`CREATE TABLE IF NOT EXISTS echosync_scope_changes(scope text NOT NULL, revision bigint NOT NULL, aggregate text NOT NULL,id text NOT NULL,PRIMARY KEY(scope,revision,aggregate,id))`
  // Engine writers hold the singleton lock and advance it once after all stream mutations.
  // Store identities, not historical state: catchup returns only the authorized current view.
  yield* sql`CREATE OR REPLACE FUNCTION echosync_track_stream() RETURNS trigger LANGUAGE plpgsql AS $body$
    DECLARE revision bigint;
    BEGIN
      SELECT cursor+1 INTO revision FROM echosync_engine;
      IF TG_OP <> 'INSERT' THEN
        INSERT INTO echosync_scope_changes VALUES(OLD.scope,revision,OLD.aggregate,OLD.id) ON CONFLICT DO NOTHING;
      END IF;
      IF TG_OP <> 'DELETE' THEN
        INSERT INTO echosync_scope_changes VALUES(NEW.scope,revision,NEW.aggregate,NEW.id) ON CONFLICT DO NOTHING;
      END IF;
      RETURN NULL;
    END
  $body$`
  yield* sql`DROP TRIGGER IF EXISTS echosync_track_stream ON echosync_streams`
  yield* sql`CREATE TRIGGER echosync_track_stream AFTER INSERT OR UPDATE OR DELETE ON echosync_streams FOR EACH ROW EXECUTE FUNCTION echosync_track_stream()`
  yield* sql`CREATE TABLE IF NOT EXISTS echosync_pull_sessions(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),principal text NOT NULL,epoch text NOT NULL,grant_position bigint NOT NULL,expires_at timestamptz NOT NULL DEFAULT now()+interval '5 minutes',response jsonb NOT NULL)`
  yield* sql`CREATE INDEX IF NOT EXISTS echosync_pull_expiry ON echosync_pull_sessions(expires_at)`
  yield* sql`CREATE TABLE IF NOT EXISTS echosync_pull_entries(session uuid NOT NULL REFERENCES echosync_pull_sessions(id) ON DELETE CASCADE,position bigint NOT NULL,scope text NOT NULL,entry jsonb NOT NULL,PRIMARY KEY(session,position))`
})

/** Must run inside the serialized engine transaction. Never changes receipt/authority identity. */
export const compact = Effect.fnUntraced(function* (through: number) {
  const sql = yield* SqlClient.SqlClient
  yield* Schema.decodeEffect(Wire.Position)(through)
  yield* sql`INSERT INTO echosync_scope_floors(scope,cursor)
    SELECT scope,least(cursor,${through}) FROM echosync_scopes
    ON CONFLICT(scope) DO UPDATE SET cursor=greatest(echosync_scope_floors.cursor,EXCLUDED.cursor)`
  yield* sql`DELETE FROM echosync_scope_changes c USING echosync_scope_floors f WHERE c.scope=f.scope AND c.revision<=f.cursor`
})

/** Materialize a fixed view before paging; no SQL transaction or connection spans requests. */
export const pull = Effect.fnUntraced(function* (principal: Principal, request: Wire.PullRequest) {
  const sql = yield* PgClient.PgClient
  if (request.continuation === undefined) yield* sql`DELETE FROM echosync_pull_sessions WHERE expires_at<=now()`
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
      const engines = yield* sql<{
        readonly epoch: string
        readonly revision: string
      }>`SELECT epoch,cursor::text AS revision FROM echosync_engine`
      const engine = engines[0]
      if (engine === undefined) return yield* new Wire.ServerError({ message: "Run EchoSync migrate" })
      if (request.epoch !== null && request.epoch !== engine.epoch)
        return yield* new Wire.ResyncRequired({ epoch: engine.epoch })
      const auth = yield* sql<{
        readonly position: string
      }>`SELECT coalesce(max(position),0)::text AS position FROM echosync_grant_events`
      const authorization = auth[0]?.position ?? "0"
      let id = request.continuation?.id
      if (id === undefined) {
        const grants = yield* Schema.decodeUnknownEffect(Schema.Array(Grant))(
          yield* sql`SELECT scope,principal,role FROM echosync_grants WHERE principal=${principal}`
        )
        const scopes = yield* sql<{ readonly scope: string; readonly cursor: string; readonly floor: string }>`
        SELECT s.scope,s.cursor::text,coalesce(f.cursor,0)::text AS floor FROM
        (SELECT scope,cursor FROM echosync_scopes UNION SELECT ${privateScope(principal)},0 WHERE NOT EXISTS(SELECT 1 FROM echosync_scopes WHERE scope=${privateScope(principal)})) s
        LEFT JOIN echosync_scope_floors f ON f.scope=s.scope
        WHERE s.scope=${privateScope(principal)} OR s.scope LIKE 'catalog:%' OR EXISTS(SELECT 1 FROM echosync_grants g WHERE g.scope=s.scope AND g.principal=${principal}) ORDER BY s.scope`
        const response: Wire.PullResponse = {
          epoch: engine.epoch,
          revision: Number(engine.revision),
          grants,
          scopes: Arr.map(scopes, ({ scope, cursor, floor }) => ({
            scope,
            cursor: Number(cursor),
            reset:
              request.cursors[scope] === undefined ||
              (request.cursors[scope] ?? 0) < Number(floor) ||
              (request.cursors[scope] ?? 0) > Number(cursor),
            entries: []
          }))
        }
        const sessions = yield* sql<{
          readonly id: string
        }>`INSERT INTO echosync_pull_sessions(principal,epoch,grant_position,response) VALUES(${principal},${engine.epoch},${authorization},${sql.json(response)}) RETURNING id::text`
        id = sessions[0]?.id
        if (id === undefined) return yield* new Wire.ServerError({ message: "Cannot stage Pull" })
        let offset = 0
        for (const scope of response.scopes) {
          const source = scope.reset
            ? sql`SELECT ${id}::uuid,${offset}+row_number() OVER(ORDER BY aggregate,id),${scope.scope},
                jsonb_build_object('_tag','Upsert','aggregate',aggregate,'id',id,'seq',seq,'state',state)
                FROM echosync_streams WHERE scope=${scope.scope}`
            : sql`
          SELECT ${id}::uuid,${offset}+row_number() OVER(ORDER BY keys.aggregate,keys.id),${scope.scope},
            CASE WHEN s.id IS NULL THEN jsonb_build_object('_tag','Gone','aggregate',keys.aggregate,'id',keys.id)
            ELSE jsonb_build_object('_tag','Upsert','aggregate',s.aggregate,'id',s.id,'seq',s.seq,'state',s.state) END
          FROM (
            SELECT DISTINCT aggregate,id FROM echosync_scope_changes WHERE scope=${scope.scope} AND revision>${request.cursors[scope.scope] ?? 0} AND revision<=${scope.cursor}
          ) keys LEFT JOIN echosync_streams s ON s.aggregate=keys.aggregate AND s.id=keys.id AND s.scope=${scope.scope}`
          const inserted = yield* sql<{ readonly count: string }>`WITH inserted AS (
            INSERT INTO echosync_pull_entries(session,position,scope,entry) ${source} RETURNING 1
          ) SELECT count(*)::text AS count FROM inserted`
          offset += Number(inserted[0]?.count ?? 0)
        }
      }
      const sessions = yield* sql<{
        readonly response: unknown
      }>`SELECT response FROM echosync_pull_sessions WHERE id=${id}::uuid AND principal=${principal} AND epoch=${engine.epoch} AND grant_position=${authorization} AND expires_at>now()`
      const session = sessions[0]
      // Expiry/revocation is retryable without resetting the durable replica or journal.
      if (session === undefined)
        return yield* new Wire.ServerError({ message: "Pull continuation expired; restart catchup" })
      const response = yield* Schema.decodeUnknownEffect(Wire.PullResponse)(session.response)
      const offset = request.continuation?.offset ?? 0
      const rows = yield* sql<{
        readonly position: string
        readonly scope: string
        readonly entry: unknown
      }>`SELECT position::text,scope,entry FROM echosync_pull_entries WHERE session=${id}::uuid AND position>${offset} ORDER BY echosync_pull_entries.position LIMIT 2049`
      const page = rows.slice(0, 2048)
      const entries = new Map<string, Array<typeof Wire.Entry.Type>>()
      for (const row of page) {
        const values = entries.get(row.scope) ?? []
        values.push(yield* Schema.decodeUnknownEffect(Wire.Entry)(row.entry))
        entries.set(row.scope, values)
      }
      return {
        ...response,
        scopes: Arr.map(response.scopes, (scope) => ({ ...scope, entries: entries.get(scope.scope) ?? [] })),
        page: { id, next: rows.length > 2048 ? Number(page[page.length - 1]?.position ?? offset) : null }
      }
    })
  )
})
