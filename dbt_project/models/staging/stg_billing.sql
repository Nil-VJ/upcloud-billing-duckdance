-- Cleaned, typed, and GDPR-aware view of raw billing data.
-- credit_usage is flipped from negative to positive and renamed credit_used.

select
    timestamp                       as operation_at,
    cast(date_trunc('day', timestamp) as date) as operation_date,
    resource_id,
    user_id,
    -credit_usage                   as credit_used,
    region,
    service_tier,
    operation_type,
    success,
    cast(success as integer)        as is_success_int,
    resource_type,
    invoice_id,
    currency,
    year,
    month,
    day

from {{ source('raw', 'raw_billing') }}