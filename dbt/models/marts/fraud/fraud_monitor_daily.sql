WITH transactions AS
(
    SELECT * FROM {{ref('stg_transactions')}}
),

aggregated AS
(
    SELECT
        -- FLOOR(transaction_hour / 24) AS transaction_day,
        DATE_ADD(DATE('2024-01-01'), INTERVAL CAST(FLOOR(transaction_hour/24) AS INT64) DAY) AS transaction_date,
        
        transaction_type,

        COUNT(*) AS total_transactions,
        SUM(transaction_amount) AS total_amount,

        COUNT(CASE WHEN is_fraud THEN 1 END) AS fraud_count,
        SUM(CASE WHEN is_fraud THEN transaction_amount ELSE 0 END) AS fraud_amount,

        AVG(error_balance_origin) AS avg_error_balance,
        AVG(time_since_last_transaction) AS avg_time_gap

    FROM transactions
    GROUP BY transaction_date, transaction_type

)

SELECT 
    *,
    CASE
        WHEN total_transactions > 0 THEN fraud_count / total_transactions
        ELSE 0
    END AS fraud_rate

FROM aggregated