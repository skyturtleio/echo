import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Str from "effect/String"

// Build-time source check, not part of the engine runtime. Story.make calls are specifications;
// every it/test block must instead name an invariant that exists in the design catalogue.
const design = await Bun.file("docs/design.md").text()
const known = new Set(Array.from(Str.matchAll(/^- \*\*(INV-[A-Z]+-\d+):\*\*/gm)(design), (match) => match[1]))
const failures: Array<string> = []
const transpiler = new Bun.Transpiler({ loader: "ts" })
for (const path of new Bun.Glob("{src,test,examples}/**/*.test.ts").scanSync(".")) {
  // Transpilation strips comments and TS syntax; scenario names must be static string literals.
  const source = transpiler.transformSync(await Bun.file(path).text())
  for (const match of Str.matchAll(/\b(?:it|test)(?:\.\w+)*\s*\(\s*(?:(["'`])([^\n]*?)\1)?/g)(source)) {
    const name = match[2] ?? "<nonliteral name>"
    const ids = Option.getOrElse(Str.match(/INV-[A-Z]+-\d+/g)(name), () => [])
    if (ids.length === 0 || Arr.some(ids, (id) => !known.has(id))) {
      failures.push(`${path}: test must cite an existing invariant: ${name}`)
    }
  }
}
if (failures.length > 0) {
  await Effect.runPromise(Effect.logError(Arr.join(failures, "\n")))
  process.exitCode = 1
} else {
  await Effect.runPromise(Effect.log("Invariant names: OK"))
}
