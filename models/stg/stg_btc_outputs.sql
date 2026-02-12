{{config(
    materialized='incremental',
    incremental_strategy='append'
)}}

WITH CTE AS
(
SELECT
t.hash_key,
t.block_number,
t.BLOCK_TIMESTAMP,
t.is_CoinBase,
f.value:address::STRING as output_address,
f.value:value::FLOAT as output_value

FROM {{ ref('stg_btc') }} t,

LATERAL FLATTEN(input => OUTPUTS) AS f
WHERE f.value:address IS NOT NULL


{% if is_incremental() %}

--WHERE HASH_KEY NOT IN (SELECT HASH_KEY FROM {{ this }})
AND t.BLOCK_TIMESTAMP >= (SELECT MAX(BLOCK_TIMESTAMP) FROM {{ this }})

{%endif%}
)

SELECT * FROM CTE