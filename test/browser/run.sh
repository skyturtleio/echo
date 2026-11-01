#!/usr/bin/env bash
set -euo pipefail
# Start `bun test/browser/serve.ts` first (supervised service in an orb).
session=echo-invariants
profile="run-$(date +%s)-$$"
trap 'agent-browser --session "$session" close >/dev/null 2>&1 || true' EXIT
agent-browser --session "$session" open "${ECHO_BROWSER_URL:-http://localhost:4178}"
agent-browser --session "$session" set viewport 1280 720 2
agent-browser --session "$session" wait --fn 'typeof echoScenarios === "object"'
agent-browser --session "$session" eval "echoScenarios.prepareReload('$profile-reload')"
agent-browser --session "$session" reload
agent-browser --session "$session" wait --fn 'typeof echoScenarios === "object"'
agent-browser --session "$session" eval "echoScenarios.verifyReload('$profile-reload')"
for scenario in lostReceipt rebaseAndEpoch lifetime queries diagnostics batches ownership; do
  agent-browser --session "$session" eval "echoScenarios.$scenario('$profile-$scenario')"
done
agent-browser --session "$session" wait --fn 'navigator.serviceWorker.controller !== null'
agent-browser --session "$session" eval "echoScenarios.prepareServerReload('$profile-server')"
agent-browser --session "$session" set offline on
agent-browser --session "$session" eval "echoScenarios.pendingServerWrite('$profile-server')"
agent-browser --session "$session" reload
agent-browser --session "$session" wait --fn 'typeof echoScenarios === "object"'
agent-browser --session "$session" eval "echoScenarios.verifyServerReload('$profile-server')"
agent-browser --session "$session" set offline off
agent-browser --session "$session" eval "echoScenarios.verifyServerReload('$profile-server')"
curl --fail --silent --show-error "${ECHO_BROWSER_URL:-http://localhost:4178}/seed-catalog"
agent-browser --session "$session" eval "echoScenarios.largeCatalog('$profile-catalog')"
agent-browser --session "$session" reload
agent-browser --session "$session" wait --fn 'typeof echoScenarios === "object"'
agent-browser --session "$session" eval "echoScenarios.largeCatalog('$profile-catalog', true)"
agent-browser --session "$session" eval "echoScenarios.oversizedCatalog('$profile-catalog')"
