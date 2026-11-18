# Boccob lakehouse - solution repository. Every target wraps a script in scripts/.
SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help
SOURCE ?= borgerportal

.PHONY: help setup tools tools-supply-chain lint test opa-test dbt-parse classification check-classification \
        bundle bundle-push image image-push harness-up harness-down integration \
        ingest dbt-build migrate migrate-status erase-dry-run retention-dry-run maintain dagster-dev clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-22s %s\n", $$1, $$2}'

setup: ## Create the virtualenv from the lockfile (uv)
	uv sync --frozen

tools: ## Install pinned opa + oras into .tools/bin
	scripts/install-tools.sh opa oras

tools-supply-chain: ## Install pinned syft, trivy, cosign into .tools/bin
	scripts/install-tools.sh syft trivy cosign

lint: ## ruff, shellcheck, opa fmt/check, dbt parse, PII classification check
	scripts/lint.sh

test: ## Unit tests: pytest, opa test, dbt parse, dagster definitions validate
	scripts/test.sh

opa-test: ## OPA policy unit tests only
	source scripts/lib.sh && opa test -v governance/policies

dbt-parse: ## Parse the dbt project (writes transform/target/manifest.json)
	scripts/dbt.sh parse

classification: ## Regenerate the OPA classification data from the dbt manifest
	scripts/classification.sh generate

check-classification: ## Fail if a clean+ column is unclassified or the OPA data is stale
	scripts/classification.sh check

bundle: ## Build the OPA policy bundle -> dist/opa/bundle.tar.gz
	scripts/build-opa-bundle.sh

bundle-push: ## Push the OPA bundle as OCI artifact (REGISTRY=...)
	scripts/push-opa-bundle.sh

image: ## Build the Dagster code location image (+ SBOM/scan if tools present)
	scripts/build-image.sh

image-push: ## Push the image to the registry (REGISTRY=...) and record the digest
	scripts/push-image.sh

harness-up: ## Start the local integration harness (podman: RustFS, Polaris, OPA, Trino)
	tests/integration/harness.sh up

harness-down: ## Remove the local integration harness
	tests/integration/harness.sh down

integration: ## Full end-to-end test on the harness (starts and removes it)
	scripts/integration-test.sh

ingest: ## Land + index one delivery (SOURCE=borgerportal)
	scripts/run.sh lakehouse-ingest run $(SOURCE)

dbt-build: ## dbt build of clean, consolidated, curated (models + tests)
	scripts/dbt.sh build

migrate: ## Apply pending schema migrations (transform/migrations), see docs/releases.md
	scripts/run.sh lakehouse-migrate

migrate-status: ## List applied and pending schema migrations (changes nothing)
	scripts/run.sh lakehouse-migrate --dry-run

erase-dry-run: ## Report what an erasure would remove, read-only (ERASURE_IDENTIFIER=... in the environment)
	scripts/run.sh lakehouse-erase --dry-run --requested-by "$${USER:-operator}" --reason dry-run

retention-dry-run: ## Report what the retention policy would delete
	scripts/run.sh lakehouse-retention --dry-run

maintain: ## Iceberg maintenance for all layers
	scripts/run.sh lakehouse-maintain

dagster-dev: ## Local Dagster UI against the configured environment
	scripts/run.sh dagster dev -m orchestration.definitions

clean: ## Remove build outputs (dist/, dbt target and logs)
	git clean -fdX -- dist transform/target transform/logs
