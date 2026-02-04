WITH source AS
(
    SELECT * FROM {{ source('gcp_fraud_source', 'raw_transactions')}}
),

renamed AS
(
    SELECT
        step AS transaction_hour,
        type AS transaction_type,

        nameOrig AS customer_id,
        nameDest AS recipient_id,

        amount AS transaction_amount,
        oldbalanceOrg AS old_balance_org,
        newbalanceOrig AS new_balance_org,
        oldbalanceDest AS old_balance_dest,
        newbalanceDest AS new_balance_dest,

        -- Engineered features
        errorBalanceOrig AS error_balance_origin,
        stepDiff AS time_since_last_transaction,
        avgAmtLast3 AS avg_amount_recent_3,

        -- Target
        CAST(isFraud AS BOOLEAN) AS is_fraud,
        CAST(isFlaggedFraud AS BOOLEAN) AS is_flagged_fraud
    
    FROM source
)

SELECT * FROM renamed