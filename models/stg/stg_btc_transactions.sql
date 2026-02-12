{{config(materialized='ephemeral')}}

select 
*
FROM {{ ref('stg_btc_outputs') }} 
Where is_CoinBase = false