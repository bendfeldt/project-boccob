{#
  Full refresh guard (audit N-5).

  A full refresh rebuilds an incremental model from the raw index, which keeps only the last
  `index_days` (90) of deliveries: clean would lose the citizen change history and the 5-year
  bookkeeping rows, consolidated/curated would lose SCD2 versions and the anonymised facts of
  erased subjects, while the keystore keeps keys that no dimension row refers to any more.
  Outside dev (LAKEHOUSE_ENV != 'dev', unset counts as production) a full refresh of an
  incremental model is therefore refused. Schema changes go through versioned migrations
  (transform/migrations, Dagster job `migrate`, docs/releases.md).

  Two layers:
  * on-run-start: `guard_full_refresh()` aborts the whole invocation before anything runs
  * `should_full_refresh()` (overrides the dbt built-in): a backstop inside every materialization,
    also catching a `full_refresh: true` model config
#}

{% macro lakehouse_env() %}
  {{ return(env_var('LAKEHOUSE_ENV', '') | trim | lower) }}
{% endmacro %}


{% macro full_refresh_blocked(materialized, full_refresh=true, environment=none) %}
  {%- set environment = lakehouse_env() if environment is none else environment -%}
  {{ return(full_refresh and materialized == 'incremental' and environment != 'dev') }}
{% endmacro %}


{% macro full_refresh_error(models) %}
  {{ exceptions.raise_compiler_error(
      "Full refresh of incremental model(s) " ~ (models | join(', ')) ~ " is refused outside dev "
      ~ "(LAKEHOUSE_ENV='" ~ lakehouse_env() ~ "'): it would drop history that the raw layer no longer "
      ~ "holds. Ship a migration instead (transform/migrations, docs/releases.md)."
  ) }}
{% endmacro %}


{% macro guard_full_refresh() %}
  {%- if execute and flags.FULL_REFRESH -%}
    {%- set blocked = [] -%}
    {%- for uid in selected_resources -%}
      {%- set node = graph.nodes.get(uid) -%}
      {%- if node and node.resource_type == 'model' and full_refresh_blocked(node.config.materialized) -%}
        {%- do blocked.append(node.name) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if blocked -%}
      {%- do full_refresh_error(blocked) -%}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}


{% macro should_full_refresh() %}
  {#- same as dbt's built-in, plus the guard -#}
  {%- set config_full_refresh = config.get('full_refresh') -%}
  {%- if config_full_refresh is none -%}
    {%- set config_full_refresh = flags.FULL_REFRESH -%}
  {%- endif -%}
  {%- if full_refresh_blocked(config.get('materialized'), config_full_refresh) -%}
    {%- do full_refresh_error([this.identifier]) -%}
  {%- endif -%}
  {{ return(config_full_refresh) }}
{% endmacro %}


{# `dbt run-operation full_refresh_guard_selftest` - checks the decision table without a database #}
{% macro full_refresh_guard_selftest() %}
  {%- set cases = [
      ['incremental', true, 'dev', false],
      ['incremental', true, 'test', true],
      ['incremental', true, 'prod', true],
      ['incremental', true, '', true],
      ['incremental', false, 'prod', false],
      ['table', true, 'prod', false],
      ['dimension', true, 'prod', false],
  ] -%}
  {%- for m, fr, e, expected in cases -%}
    {%- if full_refresh_blocked(m, fr, e) != expected -%}
      {{ exceptions.raise_compiler_error("guard selftest failed for " ~ [m, fr, e] | join('/')) }}
    {%- endif -%}
  {%- endfor -%}
  {{ log("full refresh guard selftest ok (" ~ cases | length ~ " cases)", info=true) }}
{% endmacro %}


{#
  Type change guard (audit R3-4). With on_schema_change='append_new_columns' dbt adds new columns
  but silently ignores a changed data type (the merge then casts implicitly or fails late). Refuse
  it: a retype of a released incremental model is a migration (docs/releases.md).
  Found by dispatch (dbt_project.yml: dispatch search order boccob -> dbt).
#}
{% macro trino__process_schema_changes(on_schema_change, source_relation, target_relation) %}
  {%- if on_schema_change == 'append_new_columns' -%}
    {%- set changes = check_for_schema_changes(source_relation, target_relation) -%}
    {%- if changes['new_target_types'] -%}
      {%- set described = [] -%}
      {%- for c in changes['new_target_types'] -%}
        {%- do described.append(c['column_name'] ~ ' -> ' ~ c['new_type']) -%}
      {%- endfor -%}
      {{ exceptions.raise_compiler_error(
          "Column type change in " ~ target_relation ~ ": " ~ (described | join(', '))
          ~ ". append_new_columns would ignore it - ship a migration (docs/releases.md)."
      ) }}
    {%- endif -%}
  {%- endif -%}
  {{ return(dbt.default__process_schema_changes(on_schema_change, source_relation, target_relation)) }}
{% endmacro %}


{# `dbt run-operation schema_change_guard_selftest` - the incremental materialization must reach the guard #}
{% macro schema_change_guard_selftest() %}
  {%- set resolved = adapter.dispatch('process_schema_changes', 'dbt') -%}
  {%- if resolved.get_name() != 'trino__process_schema_changes' or 'boccob' not in (resolved.macro.package_name | string) -%}
    {{ exceptions.raise_compiler_error("process_schema_changes resolves to " ~ resolved.macro.package_name ~ "." ~ resolved.get_name()) }}
  {%- endif -%}
  {{ log("schema change guard selftest ok (" ~ resolved.macro.package_name ~ "." ~ resolved.get_name() ~ ")", info=true) }}
{% endmacro %}
