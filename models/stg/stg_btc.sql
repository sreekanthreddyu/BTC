{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='HASH_KEY'
)}}

SELECT
*
FROM {{source('btc', 'btc')}}

{% if is_incremental() %}

WHERE HASH_KEY NOT IN (SELECT HASH_KEY FROM {{ this }})
--WHERE BLOCK_TIMESTAMP >= (SELECT MAX(BLOCK_TIMESTAMP) FROM {{ this }})

{%endif%}