{# Reusable generic data tests (kept in-repo: the platform is air-gapped, no package downloads). #}

{% test unique_combination(model, columns) %}
  select {{ columns | join(', ') }}, count(*) as n
  from {{ model }}
  group by {{ columns | join(', ') }}
  having count(*) > 1
{% endtest %}

{# Pseudonymised keys are 64 lower-case hex characters (HMAC-SHA256). #}
{% test pseudonym_format(model, column_name) %}
  select {{ column_name }} from {{ model }}
  where {{ column_name }} is not null and not regexp_like({{ column_name }}, '^[0-9a-f]{64}$')
{% endtest %}

{% test cpr_shape(model, column_name) %}
  select {{ column_name }} from {{ model }}
  where {{ column_name }} is not null and not regexp_like({{ column_name }}, '^\d{6}-?\d{4}$')
{% endtest %}

{# SCD2: validity intervals of one business key must not overlap and must be well formed. #}
{% test scd2_no_overlap(model, business_key) %}
  with v as (
    select {{ business_key }} as bk, valid_from, valid_to,
           lead(valid_from) over (partition by {{ business_key }} order by valid_from) as next_from
    from {{ model }}
  )
  select * from v
  where valid_from >= valid_to or (next_from is not null and next_from < valid_to)
{% endtest %}

{# SCD2: intervals are contiguous and exactly one version per business key is current. #}
{% test scd2_contiguous_one_current(model, business_key) %}
  with v as (
    select {{ business_key }} as bk, valid_to, is_current,
           lead(valid_from) over (partition by {{ business_key }} order by valid_from) as next_from
    from {{ model }}
  ),
  gaps as (
    select bk, 'gap' as problem from v where next_from is not null and next_from <> valid_to
  ),
  currents as (
    select bk, 'current_count' as problem from v group by bk
    having count_if(is_current) <> 1 or count_if(is_current and valid_to <> {{ var('scd_end_of_time') }}) > 0
  )
  select * from gaps union all select * from currents
{% endtest %}

{# Keystore integrity for one dimension: unique keys, unique (business key, version),
   every dimension row carries exactly the key the keystore holds, created timestamps set. #}
{% test keystore_integrity(model) %}
  {% set s = dimension_settings(model.identifier) %}
  {% set ks = keystore_relation(model.identifier) %}
  select 'duplicate_surrogate_key' as problem, cast(surrogate_key as varchar) as detail
  from {{ ks }} group by surrogate_key having count(*) > 1
  union all
  select 'duplicate_business_version', business_key || '|' || version_key
  from {{ ks }} group by business_key, version_key having count(*) > 1
  union all
  select 'duplicate_key_in_dimension', cast({{ s.surrogate_key }} as varchar)
  from {{ model }} group by {{ s.surrogate_key }} having count(*) > 1
  union all
  select 'dimension_key_not_in_keystore', cast(d.{{ s.surrogate_key }} as varchar)
  from {{ model }} d left join {{ ks }} k on k.surrogate_key = d.{{ s.surrogate_key }}
  where k.surrogate_key is null
  union all
  select 'dimension_key_business_key_mismatch', cast(d.{{ s.surrogate_key }} as varchar)
  from {{ model }} d join {{ ks }} k on k.surrogate_key = d.{{ s.surrogate_key }}
  where k.business_key <> cast(d.{{ s.business_key }} as varchar)
  union all
  select 'missing_created_at_or_non_positive_key', cast(surrogate_key as varchar)
  from {{ ks }} where key_created_at is null or (surrogate_key < 1 and member_type <> 'unknown')
  union all
  select 'unknown_member_missing', '-1'
  where not exists (select 1 from {{ model }} where {{ s.surrogate_key }} = -1)
{% endtest %}

{# Fact -> keystore usage semantics: every key the fact references exists in the dimension and
   has key_last_used_in_fact_at >= key_created_at. #}
{% test keystore_usage(model, column_name, dimension) %}
  {% set ks = keystore_relation(dimension) %}
  {% set s = dimension_settings(dimension) %}
  with used as (select distinct {{ column_name }} as sk from {{ model }})
  select 'fact_key_not_in_dimension' as problem, cast(u.sk as varchar) as detail
  from used u left join {{ ref(dimension) }} d on d.{{ s.surrogate_key }} = u.sk
  where d.{{ s.surrogate_key }} is null
  union all
  select 'key_used_but_last_used_not_recorded', cast(u.sk as varchar)
  from used u join {{ ks }} k on k.surrogate_key = u.sk
  where k.key_last_used_in_fact_at is null or k.key_last_used_in_fact_at < k.key_created_at
{% endtest %}

{# Keys never change: compares with the baseline stored by `keystore_snapshot_baseline`
   (passes trivially when no baseline exists). #}
{% test keystore_stable_since_baseline(model) %}
  {% set s = dimension_settings(model.identifier) %}
  {% set baseline = keystore_relation(model.identifier ~ '__baseline') %}
  {% if execute and adapter.get_relation(baseline.database, baseline.schema, baseline.identifier) is not none %}
    select b.surrogate_key, b.business_key, b.valid_from
    from {{ baseline }} b
    left join {{ model }} d
      on cast(d.{{ s.business_key }} as varchar) = cast(b.business_key as varchar)
      {% if s.scd_type == 2 %}and d.valid_from = b.valid_from{% endif %}
      and d.{{ s.surrogate_key }} = b.surrogate_key
    where d.{{ s.surrogate_key }} is null
      -- members erased since the baseline are intentionally anonymised
      and not exists (
        select 1 from {{ keystore_relation(model.identifier) }} k
        where k.surrogate_key = b.surrogate_key and k.member_type = 'erased'
      )
  {% else %}
    select 1 as no_baseline where false
  {% endif %}
{% endtest %}

{# Published aggregates must not contain cells below the disclosure threshold unless flagged. #}
{% test expression_is_true(model, expression) %}
  select * from {{ model }} where not ({{ expression }})
{% endtest %}
