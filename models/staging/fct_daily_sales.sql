WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    DATE_TRUNC('day', event_timestamp) as sale_date,
    currency,
    status,
    COUNT(transaction_id) as total_transactions,
    SUM(amount) as total_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM transactions
WHERE status = 'completed' -- Solo lo que sí se cobró
GROUP BY 1, 2, 3
ORDER BY 1 DESC