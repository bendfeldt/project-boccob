{#
  Dimensional keystore - generic, reusable macros.

  One Iceberg table per dimension in <keystore_catalog>.<keystore_schema>.<dimension>:

    surrogate_key              stable sequential integer, allocated once, never changed or reused
    business_key               business key of the member (pseudonymised where it is a person)
    version_key                SCD2: effective-from of the version (UTC text); SCD1: ''
    member_type                regular | inferred | unknown | erased
    key_created_at / _by       when and by which dbt invocation the key was allocated
    key_last_used_in_fact_at   last time a fact load referenced the key (+ which fact)
    adopted_at                 when an inferred member was replaced by real master data

  The keystore is the only stateful part of a dimension: the dimension table itself is a pure
  projection of its source and can be dropped and rebuilt at any time without changing keys.
  See docs/keystore.md for the allocation strategy and its limits.
#}

{% macro keystore_relation(dimension) -%}
  {{ return(api.Relation.create(database=env_var('CURATED_CATALOG', 'curated'), schema=var('keystore_schema'), identifier=dimension, type='table')) }}
{%- endmacro %}

{% macro keystore_inferred_version() -%} '__inferred__' {%- endmacro %}

{% macro keystore_ensure(dimension) %}
  {% set ks = keystore_relation(dimension) %}
  {% do adapter.create_schema(ks.without_identifier()) %}
  {% call statement('keystore_create') %}
    create table if not exists {{ ks }} (
      surrogate_key {{ var('keystore_key_type') }} not null,
      business_key varchar not null,
      version_key varchar not null,
      member_type varchar not null,
      key_created_at timestamp(6) with time zone not null,
      key_created_by varchar,
      key_last_used_in_fact_at timestamp(6) with time zone,
      key_last_used_by varchar,
      adopted_at timestamp(6) with time zone
    )
    comment 'Keystore of dimension {{ dimension }}: business key (+ version) -> stable integer surrogate key'
  {% endcall %}
  {% call statement('keystore_unknown') %}
    insert into {{ ks }}
    select -1, '__unknown__', '', 'unknown', current_timestamp, '{{ invocation_id }}', null, null, null
    where not exists (select 1 from {{ ks }} where surrogate_key = -1)
  {% endcall %}
  {{ return(ks) }}
{% endmacro %}

{# Number of integrity violations: duplicate surrogate keys or duplicate (business key, version). #}
{% macro keystore_violations(ks) %}
  {% set q %}
    select
      (select count(*) from (select surrogate_key from {{ ks }} group by 1 having count(*) > 1))
      + (select count(*) from (select business_key, version_key from {{ ks }} group by 1, 2 having count(*) > 1))
  {% endset %}
  {{ return(run_query(q).columns[0].values()[0] | int) }}
{% endmacro %}

{#
  Allocate keys for candidates that have none.

  candidates_sql: query returning (business_key varchar, version_key varchar), any number of rows.
  member_type:    'regular' (dimension load) or 'inferred' (fact load found an unknown member).

  Optimistic, verified allocation: one INSERT computes max(surrogate_key) + row_number() for the
  missing candidates (a single atomic Iceberg commit). Afterwards integrity is verified; if a
  concurrent writer produced a collision the rows of *this* attempt are removed and the allocation
  is retried. Keys that existed before the attempt are never touched.
#}
{% macro keystore_allocate(dimension, candidates_sql, member_type='regular') %}
  {% set ks = keystore_ensure(dimension) %}
  {% set max_attempts = var('keystore_max_attempts') | int %}

  {% if member_type == 'regular' %}
    {# real master data arrived for inferred members: they keep their key (earliest version adopts it) #}
    {% call statement('keystore_adopt') %}
      merge into {{ ks }} t
      using (
        select c.business_key, min(c.version_key) as version_key
        from ({{ candidates_sql }}) c
        join {{ ks }} k on k.business_key = c.business_key and k.member_type = 'inferred'
        group by c.business_key
      ) s
      on t.business_key = s.business_key and t.member_type = 'inferred'
      when matched then update set
        version_key = s.version_key, member_type = 'regular', adopted_at = current_timestamp
    {% endcall %}
  {% endif %}

  {% set ns = namespace(done=false, allocated=0) %}
  {% for attempt in range(1, max_attempts + 1) if not ns.done %}
    {% set allocation_id = invocation_id ~ ':' ~ dimension ~ ':' ~ member_type ~ ':' ~ attempt %}
    {% call statement('keystore_insert', fetch_result=true) %}
      insert into {{ ks }}
      select
        cast((select greatest(coalesce(max(surrogate_key), 0), 0) from {{ ks }})
             + row_number() over (order by c.business_key, c.version_key) as {{ var('keystore_key_type') }}),
        c.business_key,
        c.version_key,
        '{{ member_type }}',
        current_timestamp,
        '{{ allocation_id }}',
        null, null, null
      from (select distinct business_key, version_key from ({{ candidates_sql }}) where business_key is not null) c
      where not exists (
        select 1 from {{ ks }} k
        where k.business_key = c.business_key
          {% if member_type == 'inferred' %}
          {# any existing key of the member makes inference unnecessary #}
          {% else %}
          and k.version_key = c.version_key
          {% endif %}
      )
    {% endcall %}
    {% if keystore_violations(ks) == 0 %}
      {% set ns.done = true %}
    {% else %}
      {{ log("keystore " ~ ks ~ ": collision detected on attempt " ~ attempt ~ ", rolling back own allocation", info=true) }}
      {% call statement('keystore_rollback') %}
        delete from {{ ks }} where key_created_by = '{{ allocation_id }}'
      {% endcall %}
    {% endif %}
  {% endfor %}
  {% if not ns.done %}
    {{ exceptions.raise_compiler_error("keystore " ~ ks ~ ": could not allocate keys without collision after " ~ max_attempts ~ " attempts") }}
  {% endif %}
  {{ return(ks) }}
{% endmacro %}

{# ---- helpers for fact models ------------------------------------------------------------- #}

{% macro _dimension_node(dimension) %}
  {% set matches = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | selectattr('name', 'equalto', dimension) | list %}
  {% if matches | length != 1 %}
    {{ exceptions.raise_compiler_error("dimension model '" ~ dimension ~ "' not found") }}
  {% endif %}
  {{ return(matches[0]) }}
{% endmacro %}

{% macro dimension_settings(dimension) %}
  {% if not execute %}
    {# parse phase: the graph is not built yet; only dependencies (ref) matter #}
    {{ return({'business_key': 'business_key', 'surrogate_key': 'surrogate_key', 'scd_type': 2}) }}
  {% endif %}
  {% set cfg = _dimension_node(dimension).config %}
  {% if cfg.get('materialized') != 'dimension' %}
    {{ exceptions.raise_compiler_error("model '" ~ dimension ~ "' is not materialized as a dimension") }}
  {% endif %}
  {{ return({
      'business_key': cfg.get('business_key'),
      'surrogate_key': cfg.get('surrogate_key', dimension | replace('dim_', '') ~ '_key'),
      'scd_type': cfg.get('scd_type', var('scd_default_type')) | int,
  }) }}
{% endmacro %}

{#
  LEFT JOIN a dimension in a fact model.
    at: event time used to pick the SCD2 version valid at that moment (ignored for SCD1).
#}
{% macro dim_join(dimension, alias, business_key, at=none) -%}
  {%- set s = dimension_settings(dimension) -%}
  left join {{ ref(dimension) }} {{ alias }}
    on {{ alias }}.{{ s.business_key }} = {{ business_key }}
  {%- if s.scd_type == 2 %}
    {%- if at is none %}
    and {{ alias }}.is_current
    {%- else %}
    and {{ ts6(at) }} >= {{ alias }}.valid_from and {{ ts6(at) }} < {{ alias }}.valid_to
    {%- endif %}
  {%- endif %}
{%- endmacro %}

{# Surrogate key from a joined dimension; -1 = unknown member (no business key delivered). #}
{% macro dim_key(dimension, alias) -%}
  coalesce({{ alias }}.{{ dimension_settings(dimension).surrogate_key }}, -1)
{%- endmacro %}

{#
  Pre-hook for fact models: members referenced by the fact source that the dimension does not
  know yet (late-arriving dimension data) get an *inferred* key and a placeholder dimension row,
  so the fact row is loaded now and keeps its key when the master data arrives.
#}
{% macro keystore_register_inferred(dimension, source_relation, business_key) %}
  {% if execute %}
    {% set s = dimension_settings(dimension) %}
    {% set dim = ref(dimension) %}
    {% set candidates %}
      select cast({{ business_key }} as varchar) as business_key, {{ keystore_inferred_version() }} as version_key
      from {{ source_relation }}
      where {{ business_key }} is not null
    {% endset %}
    {% set ks = keystore_allocate(dimension, candidates, member_type='inferred') %}
    {% call statement('dimension_inferred_rows') %}
      insert into {{ dim }} ({{ s.surrogate_key }}, {{ s.business_key }}, is_inferred, is_erased
        {%- if s.scd_type == 2 %}, valid_from, valid_to, is_current{% endif %})
      select k.surrogate_key, k.business_key, true, false
        {%- if s.scd_type == 2 %}, {{ var('scd_begin_of_time') }}, {{ var('scd_end_of_time') }}, true{% endif %}
      from {{ ks }} k
      where k.member_type = 'inferred'
        and not exists (select 1 from {{ dim }} d where d.{{ s.surrogate_key }} = k.surrogate_key)
    {% endcall %}
  {% endif %}
  select 1
{% endmacro %}

{#
  Post-hook for fact models: stamp key_last_used_in_fact_at for every key referenced by rows this
  invocation wrote (fact models carry _invocation_id through audit_columns()).
#}
{% macro keystore_mark_used(dimension, key_column) %}
  {% if execute %}
    {% set ks = keystore_relation(dimension) %}
    {% call statement('keystore_mark_used') %}
      merge into {{ ks }} t
      using (
        select distinct {{ key_column }} as surrogate_key from {{ this }}
        where _invocation_id = '{{ invocation_id }}'
      ) s
      on t.surrogate_key = s.surrogate_key
      when matched then update set
        key_last_used_in_fact_at = current_timestamp, key_last_used_by = '{{ this.schema }}.{{ this.identifier }}'
    {% endcall %}
  {% endif %}
  select 1
{% endmacro %}

{# run-operation: store the current key assignment of a dimension as a baseline for the
   stability test (tests/keystore/assert_keys_stable_since_baseline.sql). #}
{% macro keystore_snapshot_baseline(dimensions) %}
  {% for dimension in dimensions %}
    {% set s = dimension_settings(dimension) %}
    {% set node = _dimension_node(dimension) %}
    {% set dim = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) %}
    {% set baseline = keystore_relation(dimension ~ '__baseline') %}
    {% call statement('baseline') %}
      create or replace table {{ baseline }} as
      select {{ s.surrogate_key }} as surrogate_key, {{ s.business_key }} as business_key,
        {% if s.scd_type == 2 %}valid_from{% else %}cast(null as timestamp(6) with time zone){% endif %} as valid_from,
        current_timestamp as baseline_at
      from {{ dim }}
    {% endcall %}
    {{ log("baseline stored in " ~ baseline, info=true) }}
  {% endfor %}
{% endmacro %}
