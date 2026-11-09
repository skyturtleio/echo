import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Str from "effect/String"
import * as Ast from "typescript/ast"
import { API } from "typescript/async"

// A source admission check, not execution/coverage proof. Keep declarations explicit:
// unaliased it/test, direct modifiers, literal names; Story.make is a separate specification.
// Browser support is deliberately coupled to the current registry and shell runner shapes.
const design = await Bun.file("docs/design.md").text()
const known = new Set(Array.from(Str.matchAll(/^- \*\*(INV-[A-Z]+-\d+):\*\*/gm)(design), (match) => match[1]))
const cited = new Set<string>()
const failures: Array<string> = []
const api = new API()
const visit = (node: Ast.Node, f: (node: Ast.Node) => void): void => {
  f(node)
  node.forEachChild((child) => visit(child, f))
}
const cite = (path: string, name: string): void => {
  const ids = Array.from(Str.matchAll(/INV-[A-Z]+-\d+/g)(name), (match) => match[0])
  if (ids.length === 0 || Arr.some(ids, (id) => !known.has(id))) {
    failures.push(`${path}: test must cite an existing invariant: ${name}`)
  }
  for (const id of ids) cited.add(id)
}
try {
  // Inspect parser-delimited comments, never text inside strings/templates/regexes.
  const sources = [
    ...new Bun.Glob("{src,test,examples}/**/*.ts").scanSync("."),
    ...new Bun.Glob("**/*.ts").scanSync({ cwd: ".agents", absolute: true }),
    ...new Bun.Glob("*.ts").scanSync(".")
  ]
  for (const path of sources) {
    const source = await Bun.file(path).text()
    await using lease = await api.createSourceFile(path, source)
    visit(lease.sourceFile, (node) => {
      if (
        Ast.isImportDeclaration(node) &&
        Ast.isStringLiteral(node.moduleSpecifier) &&
        /^echo(?:-sync)?(?:\/|$)/.test(node.moduleSpecifier.text)
      ) {
        failures.push(`${path}: use canonical echosync imports, not old package names`)
      }
      if (Ast.isIdentifier(node) && /^(?:EchoRpcs|echoScenarios|ECHO_[A-Z_]+)$/.test(node.text)) {
        failures.push(`${path}: use canonical EchoSync / echosync / ECHOSYNC identifiers`)
      }
    })
    const comments = new Map<number, Ast.CommentRange>()
    const collect = (node: Ast.Node): void => {
      for (const range of [
        ...(Ast.getLeadingCommentRanges(source, node.pos) ?? []),
        ...(Ast.getTrailingCommentRanges(source, node.end) ?? [])
      ])
        comments.set(range.pos, range)
      for (const child of node.getChildren()) collect(child)
    }
    collect(lease.sourceFile)
    for (const range of comments.values()) {
      const comment = Str.trim(
        Str.slice(range.pos + 2, range.end - (range.kind === Ast.SyntaxKind.MultiLineCommentTrivia ? 2 : 0))(source)
      )
      if (/@effect-diagnostics\b/.test(comment)) {
        failures.push(`${path}:${range.pos}: use a reasoned oxlint exception, not @effect-diagnostics`)
      }
      if (
        /\b(?:oxlint|eslint)-disable\b/.test(comment) &&
        !/^(?:oxlint|eslint)-disable(?:-next-line|-line)?\s+[\w/-]+(?:\s*,\s*[\w/-]+)*\s+--\s+\S[^\r\n]*$/.test(comment)
      ) {
        failures.push(`${path}:${range.pos}: lint disables require specific rules and inline -- reason`)
      }
    }
  }
  for (const path of new Bun.Glob("{src,test,examples}/**/*.test.ts").scanSync(".")) {
    await using lease = await api.createSourceFile(path, await Bun.file(path).text())
    visit(lease.sourceFile, (node) => {
      if (Ast.isImportDeclaration(node)) {
        if (Ast.isStringLiteral(node.moduleSpecifier) && /^(?:@effect\/)?vitest$/.test(node.moduleSpecifier.text)) {
          const clause = node.importClause
          const bindings = clause?.namedBindings
          if (clause?.name !== undefined || (bindings !== undefined && !Ast.isNamedImports(bindings))) {
            failures.push(`${path}: use named, unaliased test imports`)
          }
          if (bindings !== undefined && Ast.isNamedImports(bindings)) {
            for (const spec of bindings.elements) {
              if (spec.propertyName !== undefined)
                failures.push(`${path}: aliased test-library imports are unsupported`)
            }
          }
        }
      }
      if (!Ast.isIdentifier(node) || (node.text !== "it" && node.text !== "test")) return
      if (Ast.isImportSpecifier(node.parent)) return
      let callee: Ast.Node = node
      while (Ast.isPropertyAccessExpression(callee.parent) && callee.parent.expression === callee) {
        callee = callee.parent
        if (
          Ast.isPropertyAccessExpression(callee) &&
          !/^(?:effect|live|scoped|scopedLive|skip|only|concurrent|sequential|fails)$/.test(callee.name.text)
        ) {
          failures.push(`${path}: unsupported test modifier/factory: ${callee.getText()}`)
        }
      }
      const call = callee.parent
      if (!Ast.isCallExpression(call) || call.expression !== callee) {
        failures.push(`${path}: it/test must be called directly, not aliased or computed`)
        return
      }
      const name = call.arguments[0]
      if (name === undefined || (!Ast.isStringLiteral(name) && !Ast.isNoSubstitutionTemplateLiteral(name))) {
        failures.push(`${path}: test name must be a string literal`)
      } else {
        cite(path, name.text)
      }
    })
  }

  const path = "test/browser/client.ts"
  await using browser = await api.createSourceFile(path, await Bun.file(path).text())
  const definitions = new Map<string, Ast.Node>()
  const registered = new Set<string>()
  visit(browser.sourceFile, (node) => {
    if (Ast.isVariableDeclaration(node) && Ast.isIdentifier(node.name) && node.initializer !== undefined) {
      definitions.set(node.name.text, node.initializer)
    }
    if (!Ast.isPropertyAssignment(node) || node.name.getText() !== "echosyncScenarios") return
    if (!Ast.isObjectLiteralExpression(node.initializer)) {
      failures.push(`${path}: echosyncScenarios must be an explicit object`)
      return
    }
    for (const entry of node.initializer.properties) {
      if (
        !Ast.isPropertyAssignment(entry) ||
        !Ast.isIdentifier(entry.name) ||
        !Ast.isArrowFunction(entry.initializer)
      ) {
        failures.push(`${path}: browser registration must be name: (...) => run(name(...))`)
        continue
      }
      const name = entry.name.text
      const body = entry.initializer.body
      const inner = Ast.isCallExpression(body) ? body.arguments[0] : undefined
      if (
        !Ast.isCallExpression(body) ||
        body.expression.getText() !== "run" ||
        inner === undefined ||
        !Ast.isCallExpression(inner) ||
        inner.expression.getText() !== name
      ) {
        failures.push(`${path}: browser registration ${name} must call run(${name}(...))`)
      }
      registered.add(name)
    }
  })
  const runner = await Bun.file("test/browser/run.sh").text()
  const invoked = new Set<string>()
  // Recognize actual eval command lines, not comments or arbitrary mentions. This is not a shell interpreter.
  for (const match of Str.matchAll(/^agent-browser .* eval "echosyncScenarios\.([A-Za-z]+)\(/gm)(runner)) {
    if (match[1] !== undefined) invoked.add(match[1])
  }
  for (const match of Str.matchAll(
    /^for scenario in ([A-Za-z ]+); do\n  agent-browser .* eval "echosyncScenarios\.\$scenario\([^\n]+\ndone/gm
  )(runner)) {
    for (const name of Str.split(match[1] ?? "", " ")) invoked.add(name)
  }
  if (registered.size === 0) failures.push(`${path}: missing browser registry`)
  for (const name of registered) {
    if (!invoked.has(name)) failures.push(`test/browser/run.sh: registered scenario not invoked: ${name}`)
    const definition = definitions.get(name)
    let hasId = false
    if (definition !== undefined)
      visit(definition, (node) => {
        if (Ast.isStringLiteral(node) || Ast.isNoSubstitutionTemplateLiteral(node) || Ast.isTemplateHead(node)) {
          if (Str.startsWith("PASS INV-")(node.text)) {
            hasId = true
            cite(`${path}:${name}`, node.text)
          }
        }
      })
    if (!hasId) failures.push(`${path}:${name}: missing literal PASS invariant marker`)
  }
  for (const name of invoked) {
    if (!registered.has(name)) failures.push(`test/browser/run.sh: unregistered scenario invoked: ${name}`)
  }
  for (const id of known) {
    if (id !== undefined && !cited.has(id)) failures.push(`docs/design.md: invariant has no scenario citation: ${id}`)
  }
} finally {
  await api.close()
}
if (failures.length > 0) {
  await Effect.runPromise(Effect.logError(Arr.join(failures, "\n")))
  process.exitCode = 1
} else {
  await Effect.runPromise(Effect.log("Invariant declarations and browser runner registrations: OK (static check only)"))
}
