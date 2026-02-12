{%macro convert_to_usd(column_name) %}

{{column_name}} * (SELECT PRICE FROM {{ ref('btc_usd_max') }} WHERE to_date(to_timestamp(replace(snapped_at,' UTC','')))=CURRENT_DATE())


{%endmacro %}