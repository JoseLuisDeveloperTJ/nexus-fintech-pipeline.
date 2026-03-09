WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    DATE_TRUNC('day', event_timestamp) as sale_date,
    method,
    currency,
    status,
    COUNT(transaction_id) as total_transactions,
    SUM(amount) as total_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM transactions
WHERE status = 'completed' -- only
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC