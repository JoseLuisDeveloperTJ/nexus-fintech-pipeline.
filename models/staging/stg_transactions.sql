WITH raw_data_source AS (
     SELECT * FROM {{ source('nexus_raw', 'transactions_raw') }}
)

SELECT
     raw_data: transaction_id::string as transaction_id,
     raw_data: transaction_id::string as user_id,
     raw_data: amount::float as amount,
     raw_data: currency:string as currency,
     raw_data: status::string as status,
     raw_data: event_timestamp::timestamp_ntz as event_timestamp,

     ingested_at as loaded_at
FROM raw_data_source
