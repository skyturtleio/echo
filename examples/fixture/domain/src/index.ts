/**
 * The fixture domain the engine is built against: two aggregates and one story per engine
 * surface a domain can touch (an accepted command and its scope stamp, a read by id across
 * scopes, the generic `ScopeForbidden`, a read over an own index, a read over another
 * aggregate's index). It is not an app and has no rules beyond those; the apps' domains live
 * in their own repositories (design §5).
 */
export * from "./Note/Note.aggregate.ts"
export * from "./Note/Note.id.ts"
export * from "./Note/Note.indexes.ts"
export * from "./Note/Note.state.ts"
export * from "./Note/WriteNote.command.ts"
export * from "./Tag/DefineTag.command.ts"
export * from "./Tag/RemoveTag.command.ts"
export * from "./Tag/Tag.aggregate.ts"
export * from "./Tag/Tag.id.ts"
export * from "./Tag/Tag.indexes.ts"
export * from "./Tag/Tag.state.ts"
