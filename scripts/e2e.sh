#!/usr/bin/env bash
# End-to-end proof of this release on a running environment (dev):
#   1. loads: ingest_<source> for two partitions (initial + changed delivery), each followed by
#      the transform job, run in the cluster by Dagster
#   2. content and keystore: every layer populated, keys dense, reused by the second load, SCD2
#   3. access control for Keycloak users: analyst denied raw and sees PII masked, pii-reader clear
#   4. right to erasure (queued with the CLI in the code location pod, Dagster job `erasure`):
#      subject gone everywhere, audit row, queue empty, no key in Trino's query history
#   5. deregistration scope: person gone, payments and cases kept anonymised
#   6. only erasure runs get the purge credential; the code location itself has none; forged
#      dagster-k8s/config run tags on a non-purge job fail before any Job exists
#   7. dbt refuses a full refresh of incremental models outside dev
#
#   BOCCOB_PLATFORM_ENV=<file> make e2e            everything
#   E2E_SKIP_LOADS=1 make e2e                      reuse loads that already ran
# Everything about the environment comes from the platform environment file
# (docs/platform-contract.md, section 4). It writes test data: dev only.
# ok/bad always succeed, so "cond && ok || bad" is safe in this file
# shellcheck disable=SC2015
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need kubectl
need jq
need curl
need uv
load_platform_env BOCCOB_ENV BOCCOB_KUBECONFIG BOCCOB_CA_BUNDLE BOCCOB_TRINO_URL BOCCOB_KEYCLOAK_URL \
  BOCCOB_KEYCLOAK_REALM BOCCOB_KEYCLOAK_CLIENT_ID BOCCOB_KEYCLOAK_CLIENT_SECRET_FILE \
  BOCCOB_DAGSTER_NAMESPACE BOCCOB_CODE_LOCATION BOCCOB_CODE_LOCATION_NAME BOCCOB_DAGSTER_WEBSERVER \
  BOCCOB_DAGSTER_GRAPHQL_URL \
  BOCCOB_TEST_ANALYST_USER BOCCOB_TEST_ANALYST_PASSWORD_FILE BOCCOB_TEST_PII_READER_USER \
  BOCCOB_TEST_PII_READER_PASSWORD_FILE
[[ "$BOCCOB_ENV" == "dev" || "${ALLOW_E2E:-0}" == "1" ]] || die "e2e writes test data: dev only (ALLOW_E2E=1 to override)"
cd "$ROOT"

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
PASS=0; FAIL=0
ok()   { PASS=$((PASS + 1)); printf '  [PASS] %s\n' "$*"; }
bad()  { FAIL=$((FAIL + 1)); printf '  [FAIL] %s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
expect() { # expect <description> <expected> <actual>
  if [[ "$2" == "$3" ]]; then ok "$1 ($3)"; else bad "$1: expected $2, got $3"; fi
}

# python inside the code location pod: the release's own code and environment (svc-dagster)
pod_py() { kc exec -i "$BOCCOB_CODE_LOCATION" -- python -I -c "$1" "${@:2}"; }

# sa <sql> : one JSON array per row, as the pipeline service account
sa() {
  printf '%s' "$1" | pod_py 'import json, sys
from lakehouse.trino_client import TrinoClient
for row in TrinoClient(source="boccob-e2e").execute(sys.stdin.read()):
    print(json.dumps(row, default=str))'
}
one() { sa "$1" | head -1 | jq -r '.[0]'; }

# dagster_gql : GraphQL request (JSON on stdin), sent from inside the webserver pod
dagster_gql() {
  local container=()
  [[ -z "${BOCCOB_DAGSTER_WEBSERVER_CONTAINER:-}" ]] || container=(-c "$BOCCOB_DAGSTER_WEBSERVER_CONTAINER")
  kc exec -i "$BOCCOB_DAGSTER_WEBSERVER" "${container[@]}" -- python -I -c 'import sys, urllib.request
req = urllib.request.Request(sys.argv[1], data=sys.stdin.buffer.read(), headers={"Content-Type": "application/json"})
print(urllib.request.urlopen(req, timeout=60).read().decode())' "$BOCCOB_DAGSTER_GRAPHQL_URL"
}
launch() { # launch <job> [partition] [extra tags, JSON array of {key,value}] -> run id
  jq -n --arg job "$1" --arg p "${2:-}" --argjson extra "${3:-[]}" --arg loc "$BOCCOB_CODE_LOCATION_NAME" '{
    query: "mutation($p: ExecutionParams!) { launchRun(executionParams: $p) { __typename ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } ... on RunConfigValidationInvalid { errors { message } } ... on InvalidSubsetError { message } } }",
    variables: { p: {
      selector: { repositoryLocationName: $loc, repositoryName: "__repository__", jobName: $job },
      runConfigData: {},
      executionMetadata: { tags: ((if $p == "" then [] else [{key: "dagster/partition", value: $p}] end) + $extra) }
    } } }' | dagster_gql | jq -r '.data.launchRun | if .__typename == "LaunchRunSuccess" then .run.runId else error(tostring) end'
}
wait_run() { # wait_run <run id> [timeout s] : fails unless SUCCESS
  local id="$1" timeout="${2:-1800}" start status
  start="$(date +%s)"
  while :; do
    status="$(jq -n --arg id "$id" '{query: "query($id: ID!) { runOrError(runId: $id) { ... on Run { status } } }", variables: {id: $id}}' \
      | dagster_gql | jq -r '.data.runOrError.status')"
    case "$status" in
      SUCCESS) return 0 ;;
      FAILURE|CANCELED) log "run $id: $status"; return 1 ;;
    esac
    (( $(date +%s) - start > timeout )) && { log "run $id: still $status after ${timeout}s"; return 1; }
    sleep 15
  done
}
RUN=""
run_job() { # run_job <description> <job> [partition] : sets RUN to the run id
  RUN="$(launch "$2" "${3:-}")"
  if wait_run "$RUN"; then ok "$1: run $RUN SUCCESS"; else bad "$1: run $RUN"; fi
}

# token <user> <password file> : Keycloak access token (password grant; dev realm)
token() {
  { printf 'grant_type=password&scope=openid&client_id=%s&username=%s&' "$BOCCOB_KEYCLOAK_CLIENT_ID" "$1"
    printf 'client_secret='; python3 -I -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.stdin.read().strip()),end="")' < "$BOCCOB_KEYCLOAK_CLIENT_SECRET_FILE"
    printf '&password='; python3 -I -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.stdin.read().strip()),end="")' < "$2"
  } | curl -fsS --cacert "$BOCCOB_CA_BUNDLE" --data-binary @- \
        "$BOCCOB_KEYCLOAK_URL/realms/$BOCCOB_KEYCLOAK_REALM/protocol/openid-connect/token" | jq -r .access_token
}
# as_user <user> <token> <sql> : query through the public Trino endpoint with a bearer token
as_user() {
  local host port
  host="${BOCCOB_TRINO_URL#https://}"; host="${host%%/*}"; port="${host##*:}"; host="${host%%:*}"
  [[ "$port" != "$host" ]] || port=443
  printf '%s' "$3" | TRINO_HOST="$host" TRINO_PORT="$port" TRINO_HTTP_SCHEME=https TRINO_USER="$1" \
    TRINO_JWT_TOKEN="$2" TRINO_PASSWORD="" TRINO_CA_BUNDLE="$BOCCOB_CA_BUNDLE" uv run --no-sync python -I -c 'import json, sys
from lakehouse.trino_client import TrinoClient
for row in TrinoClient(source="boccob-e2e").execute(sys.stdin.read()):
    print(json.dumps(row, default=str))' 2>&1
}

if [[ "${E2E_SKIP_LOADS:-0}" != "1" ]]; then
  for partition in 2026-11-01 2026-11-02; do
    step "loads: ingest_borgerportal $partition, then transform"
    run_job "ingest_borgerportal $partition" ingest_borgerportal "$partition"
    run_job "transform after $partition" transform
    [[ "$partition" == 2026-11-01 ]] && keys_after_first="$(one "select max(surrogate_key) from curated.keystore.dim_citizen")"
  done
fi

step "content and keystore (service account in the code location pod)"
for t in raw.borgerportal.citizens clean.borgerportal.citizens consolidated.party.person \
         curated.borgerservice.dim_citizen curated.borgerservice.fact_payment curated.keystore.dim_citizen \
         curated.borgerservice.dp_payment_summary_v1; do
  n="$(one "select count(*) from $t")"
  if [[ "$n" =~ ^[0-9]+$ ]] && (( n > 0 )); then ok "$t populated ($n rows)"; else bad "$t: $n"; fi
done
expect "raw index holds at least two loads" true "$(one "select count(distinct _load_id) >= 2 from raw.borgerportal.citizens")"
expect "keystore keys dense and sequential" true \
  "$(one "select count(*) = max(surrogate_key) from curated.keystore.dim_citizen where surrogate_key > 0")"
expect "keystore records key creation and last use" true \
  "$(one "select count(*) > 0 and count(key_last_used_in_fact_at) > 0 and count(key_created_at) = count(*) from curated.keystore.dim_citizen where surrogate_key > 0")"
expect "dimension is SCD2 (several versions for some persons)" true \
  "$(one "select count(*) > count(distinct person_id) from curated.borgerservice.dim_citizen")"
if [[ -n "${keys_after_first:-}" ]]; then
  expect "second load reused the keys of the first" "$keys_after_first" \
    "$(one "select count(*) from curated.keystore.dim_citizen where surrogate_key between 1 and $keys_after_first")"
fi
expect "facts never fall back to the unknown citizen" 0 \
  "$(one "select count(*) from curated.borgerservice.fact_payment where citizen_key < 0")"
expect "small cells count persons" 0 \
  "$(one "select count(*) from curated.borgerservice.dp_payment_summary_v1 where is_small_cell <> (citizen_count < 5)")"

step "access control (OPA) for Keycloak users"
analyst="$BOCCOB_TEST_ANALYST_USER" reader="$BOCCOB_TEST_PII_READER_USER"
anna="$(token "$analyst" "$BOCCOB_TEST_ANALYST_PASSWORD_FILE")"
petra="$(token "$reader" "$BOCCOB_TEST_PII_READER_PASSWORD_FILE")"
expect "analyst sees catalogs curated + system only" '["curated"] ["system"]' \
  "$(as_user "$analyst" "$anna" "show catalogs" | tr '\n' ' ' | sed 's/ $//')"
out="$(as_user "$analyst" "$anna" "select payload from raw.borgerportal.citizens limit 1" || true)"
grep -q "Access Denied" <<<"$out" && ok "analyst denied on raw" || bad "analyst raw: ${out:0:200}"
q="select full_name, email from curated.borgerservice.dim_citizen where citizen_key > 0 and full_name is not null order by citizen_key limit 1"
masked="$(as_user "$analyst" "$anna" "$q")"; clear="$(as_user "$reader" "$petra" "$q")"
[[ "$(jq -r '.[0]' <<<"$masked")" == "*****" && "$(jq -r '.[1]' <<<"$masked")" == \*\*\*\*\*@* ]] \
  && ok "analyst sees PII masked (full_name=*****, email=*****@...)" || bad "analyst PII not masked: $masked"
[[ "$(jq -r '.[0]' <<<"$clear")" != "*****" && "$(jq -r '.[1]' <<<"$clear")" != \*\*\*\*\** ]] \
  && ok "pii-reader sees clear values" || bad "pii-reader got masked values: $clear"
expect "analyst cannot see the keystore schema" 0 \
  "$(as_user "$analyst" "$anna" "select count(*) from curated.information_schema.schemata where schema_name = 'keystore'" | jq -r '.[0]')"

step "right to erasure (CLI queues in the code location pod, Dagster job erasure)"
key="$(one "select c.citizen_id from clean.borgerportal.citizens c join clean.borgerportal.payments p on p.citizen_id = c.citizen_id group by c.citizen_id having count(distinct c.source_updated_at) > 1 order by c.citizen_id limit 1")"
[[ -n "$key" && "$key" != null ]] || die "no erasure candidate"
reason="e2e-$(date -u +%Y%m%dT%H%M%SZ)"
# the key goes over stdin, never on a command line; outside dev the CLI only queues
printf '%s\n' "$key" | pod_py 'import sys
from governance.erasure.cli import main
key = sys.stdin.readline().strip()
sys.exit(main(["--submit", "--subject-key", key] + sys.argv[1:]))' --requested-by "dpo@kommune.example" --reason "$reason" \
  > "$tmp/submit.json" 2>"$tmp/submit.err" && ok "erasure request queued" || bad "submit: $(tail -2 "$tmp/submit.err")"

# deregistration scope (the retention policy's path), a second subject, queued for the same batch
dkey="$(one "select c.citizen_id from clean.borgerportal.citizens c join clean.borgerportal.payments p on p.citizen_id = c.citizen_id join clean.borgerportal.cases k on k.citizen_id = c.citizen_id where c.citizen_id <> '$key' group by c.citizen_id order by c.citizen_id limit 1")"
if [[ -n "$dkey" && "$dkey" != null ]]; then
  pays="$(sa "select payment_id from clean.borgerportal.payments where citizen_id = '$dkey'" | jq -r '.[0]' | sed "s/.*/'&'/" | paste -sd, -)"
  cases="$(sa "select case_id from clean.borgerportal.cases where citizen_id = '$dkey'" | jq -r '.[0]' | sed "s/.*/'&'/" | paste -sd, -)"
  printf '%s\n' "$dkey" | pod_py 'import sys
from governance.erasure.queue import enqueue
key = sys.stdin.readline().strip()
print(enqueue(key, identifier_type="subject_key", requested_by="retention-policy", reason=sys.argv[1], scope="deregistration"))' \
    "$reason-dereg" > /dev/null 2>"$tmp/dereg.err" && ok "deregistration request queued" || bad "enqueue: $(tail -2 "$tmp/dereg.err")"
else
  bad "no deregistration candidate (citizen with payments and cases)"
fi
run_job "erasure batch (2 requests)" erasure
erasure_run="$RUN"
for check in "clean.borgerportal.citizens|citizen_id" "clean.borgerportal.payments|citizen_id" \
             "consolidated.party.person|person_id" "curated.borgerservice.dim_citizen|person_id" \
             "curated.keystore.dim_citizen|business_key"; do
  expect "subject gone from ${check%%|*}" 0 "$(one "select count(*) from ${check%%|*} where ${check##*|} = '$key'")"
done
expect "audit row of the erasure (completed)" completed \
  "$(one "select status from curated.audit.erasure_log where reason like '$reason (request %'")"
expect "erasure queue empty" 0 "$(one "select count(*) from raw.governance.erasure_requests")"
expect "no pipeline query in Trino's history carries the subject key" 0 \
  "$(one "select count(*) from system.runtime.queries where strpos(query, '$key') > 0 and source <> 'boccob-e2e'")"
if [[ -n "${dkey:-}" && "$dkey" != null ]]; then
  expect "deregistered person gone from clean.borgerportal.citizens" 0 \
    "$(one "select count(*) from clean.borgerportal.citizens where citizen_id = '$dkey'")"
  expect "payments kept anonymised (no person, no case number, _anonymised_at)" true \
    "$(one "select count(*) > 0 and count(citizen_id) = 0 and count(case_id) = 0 and count(_anonymised_at) = count(*) from clean.borgerportal.payments where payment_id in ($pays)")"
  expect "cases kept anonymised (no person, _anonymised_at)" true \
    "$(one "select count(*) > 0 and count(citizen_id) = 0 and count(_anonymised_at) = count(*) from clean.borgerportal.cases where case_id in ($cases)")"
  expect "audit row records the deregistration scope" 1 \
    "$(one "select count(*) from curated.audit.erasure_log where reason like '$reason-dereg (request %' and status = 'completed' and strpos(details, '\"scope\": \"deregistration\"') > 0")"
fi

step "purge credential only on erasure, retention and maintenance runs"
purge_secret="$(pod_py 'import os; print(os.environ.get("PURGE_CREDENTIALS_SECRET", ""))')"
expect "the code location has no PURGE_S3_* variable" 0 \
  "$(pod_py 'import os; print(sum(k.startswith("PURGE_S3") for k in os.environ))')"
envfrom() { kc get job -l "dagster/run-id=$1" -o json | jq -r '[.items[0].spec.template.spec.containers[0].envFrom[]? | .secretRef.name // empty] | join(",")'; }
if [[ -n "$purge_secret" ]]; then
  expect "the erasure run pod gets $purge_secret" yes "$([[ ",$(envfrom "$erasure_run")," == *",$purge_secret,"* ]] && echo yes || echo no)"
  run_job "migrate (a job without purge)" migrate
  expect "a migrate run pod does not" no "$([[ ",$(envfrom "$RUN")," == *",$purge_secret,"* ]] && echo yes || echo no)"
  # run-tag forgery: a Dagster UI user sets dagster-k8s/config on a non-purge job; the platform
  # (launcher allowlist, run-Job admission) must fail the run before any Job exists
  forged() { # forged <description> <dagster-k8s/config JSON>
    local run status jobs
    run="$(launch migrate "" "$(jq -cn --arg v "$2" '[{key: "dagster-k8s/config", value: $v}]')")" \
      || { bad "$1: launch failed"; return; }
    wait_run "$run" 300 >/dev/null 2>&1 || true
    status="$(jq -n --arg id "$run" '{query: "query($id: ID!) { runOrError(runId: $id) { ... on Run { status } } }", variables: {id: $id}}' \
      | dagster_gql | jq -r '.data.runOrError.status')"
    jobs="$(kc get job -l "dagster/run-id=$run" --no-headers 2>/dev/null | wc -l)"
    expect "$1" "FAILURE, 0 Job(s)" "$status, $jobs Job(s)"
  }
  purge_from='{"secret_ref": {"name": "'"$purge_secret"'"}}'
  forged "forged run tag: command override + purge Secret refused" \
    '{"container_config": {"command": ["sh", "-c", "env"], "env_from": ['"$purge_from"']}}'
  forged "forged run tag: purge Secret on a non-purge job refused" \
    '{"container_config": {"env_from": ['"$purge_from"']}}'
  forged "forged run tag: forged dagster/job label refused" \
    '{"pod_template_spec_metadata": {"labels": {"dagster/job": "erasure"}}, "container_config": {"env_from": ['"$purge_from"']}}'
else
  bad "PURGE_CREDENTIALS_SECRET is not set on the code location"
fi

step "full refresh guard"
# shellcheck disable=SC2016 # expanded inside the pod
if kc exec "$BOCCOB_CODE_LOCATION" -- env LAKEHOUSE_ENV=test sh -c \
     'cd "$DBT_PROJECT_DIR" && dbt run --full-refresh --select clean.borgerportal.citizens' > "$tmp/fr.out" 2>&1; then
  bad "dbt --full-refresh ran with LAKEHOUSE_ENV=test"
else
  grep -q "refused outside dev" "$tmp/fr.out" && ok "dbt --full-refresh refused with LAKEHOUSE_ENV=test" \
    || bad "full refresh failed for another reason: $(sed 's/\x1b\[[0-9;]*m//g' "$tmp/fr.out" | grep -m1 -A2 ERROR)"
fi

printf '\n== e2e summary (%s): %d passed, %d failed\n' "$BOCCOB_ENV" "$PASS" "$FAIL"
(( FAIL == 0 ))
