{#
  The pinball loss of one forecast at `level` (0.1 for a P10): what a range forecast is scored by.
  Below the actual it costs `level` per MW, above it `1 - level`, so a P10 is cheapest where the
  actual falls under it one time in ten. At 0.5 it is half the absolute error.
#}
{% macro pinball(level, actual, predicted) -%}
    greatest(
        {{ level }} * ({{ actual }} - {{ predicted }}),
        ({{ level }} - 1) * ({{ actual }} - {{ predicted }})
    )
{%- endmacro %}
