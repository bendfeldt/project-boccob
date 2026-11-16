{# run-operation used by the integration test to exercise concurrent allocation:
   allocates `count` new members tagged `tag` in the scratch keystore `selftest_dim`. #}
{% macro keystore_selftest(tag, count=300) %}
  {% set candidates %}
    select '{{ tag }}-' || cast(x as varchar) as business_key, '' as version_key
    from unnest(sequence(1, {{ count | int }})) as u (x)
  {% endset %}
  {% set ks = keystore_allocate('selftest_dim', candidates) %}
  {{ log("selftest allocation finished in " ~ ks, info=true) }}
{% endmacro %}
