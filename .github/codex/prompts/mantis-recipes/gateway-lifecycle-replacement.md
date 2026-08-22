# Gateway lifecycle replacement

Use this recipe when the claimed behavior crosses a Gateway restart or crash:
accepted work recovery, session lineage, stale-owner fencing, queue draining, or
post-restart delivery.

The lifecycle primitive keeps the lane's exact bounded runtime filesystem,
mock-provider container, Telegram Bot API proxy, and trusted request journals.
It replaces only the Gateway container with a new root-attested generation.

```bash
lane=baseline
repo_root="$MANTIS_BASELINE_ROOT"
config="$MANTIS_OUTPUT_DIR/lifecycle-config.json"

cat >"$config" <<'JSON'
{"mockResponse":"RECOVERED_REPLY","configPatch":{}}
JSON

$OPENCLAW_TELEGRAM_MANTIS_LANE_CMD start \
  --lane "$lane" --repo-root "$repo_root" --config "$config"

# Establish the state whose recovery is under test before this point.
$OPENCLAW_TELEGRAM_MANTIS_LANE_CMD lifecycle \
  --lane "$lane" --mode crash --timeout-seconds 60 \
  >"$MANTIS_OUTPUT_DIR/${lane}-lifecycle.json"

jq -e '
  .status == "ready" and
  .generation == (.previousGeneration + 1) and
  ([.events[].event] | index("lifecycle_requested") != null) and
  ([.events[].event] | index("gateway_exited") != null) and
  ([.events[].event] | index("gateway_ready") != null)
' "$MANTIS_OUTPUT_DIR/${lane}-lifecycle.json"

# Continue with observe/requests/botapi-requests and scenario-specific assertions.
```

Use `--mode graceful` for shutdown-drain contracts and `--mode crash` for
abrupt-loss recovery. Run the same action order in baseline and candidate.
Terminal `lifecycleEvents` are root-owned evidence: verify the request ID,
old/new generation, distinct container IDs, expected exit, termination kind, and
ready successor. Provider and Bot API journals prove whether accepted work was
replayed, lost, or delivered more than once across the same-state replacement.

If the replacement times out or records an unexpected exit, the lane is an
infrastructure failure; do not retry inside the same attempt or describe it as a
product verdict.
