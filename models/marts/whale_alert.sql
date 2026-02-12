WITH WHALES AS (

SELECT
output_address,
sum(output_value) as total_sent,
count(*) as tx_count
FROM {{ ref('stg_btc_transactions') }}
WHERE output_value > 10
GROUP BY output_address
ORDER BY total_sent DESC
)

SELECT
w.output_address,
w.total_sent,
w.tx_count,
{{ convert_to_usd('w.total_sent') }} AS total_sent_usd
FROM WHALES w

order by total_sent_usd DESC