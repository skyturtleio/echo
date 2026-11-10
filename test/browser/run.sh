#!/usr/bin/env bash
set -euo pipefail
# Start `bun test/browser/serve.ts` first (supervised service in an orb).
session=echosync-invariants
profile="run-$(date +%s)-$$"
trap 'agent-browser --session "$session" close >/dev/null 2>&1 || true' EXIT
agent-browser --session "$session" open "${ECHOSYNC_BROWSER_URL:-http://localhost:4178}"
agent-browser --session "$session" set viewport 1280 720 2
agent-browser --session "$session" wait --fn 'typeof echosyncScenarios === "object"'
agent-browser --session "$session" eval "echosyncScenarios.prepareReload('$profile-reload')"
agent-browser --session "$session" reload
agent-browser --session "$session" wait --fn 'typeof echosyncScenarios === "object"'
agent-browser --session "$session" eval "echosyncScenarios.verifyReload('$profile-reload')"
for scenario in lostReceipt rebaseAndEpoch lifetime queries diagnostics batches ownership durableRefresh compatibility incompatibleCommands payloadEvolution; do
  agent-browser --session "$session" eval "echosyncScenarios.$scenario('$profile-$scenario')"
done
agent-browser --session "$session" wait --fn 'navigator.serviceWorker.controller !== null'
agent-browser --session "$session" eval "echosyncScenarios.prepareServerReload('$profile-server')"
agent-browser --session "$session" set offline on
agent-browser --session "$session" eval "echosyncScenarios.pendingServerWrite('$profile-server')"
agent-browser --session "$session" reload
agent-browser --session "$session" wait --fn 'typeof echosyncScenarios === "object"'
agent-browser --session "$session" eval "echosyncScenarios.verifyServerReload('$profile-server')"
agent-browser --session "$session" set offline off
agent-browser --session "$session" eval "echosyncScenarios.verifyServerReload('$profile-server')"
agent-browser --session "$session" eval "echosyncScenarios.incremental('$profile-incremental')"
curl --fail --silent --show-error "${ECHOSYNC_BROWSER_URL:-http://localhost:4178}/seed-catalog"
agent-browser --session "$session" eval "echosyncScenarios.largeCatalog('$profile-catalog')"
agent-browser --session "$session" reload
agent-browser --session "$session" wait --fn 'typeof echosyncScenarios === "object"'
agent-browser --session "$session" eval "echosyncScenarios.largeCatalog('$profile-catalog', true)"
agent-browser --session "$session" eval "echosyncScenarios.oversizedCatalog('$profile-catalog')"
