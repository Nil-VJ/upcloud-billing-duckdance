-- Adds derived columns useful in marts.
-- Currently just passes staging through with redundant partition columns
-- dropped. Included to match the staging -> intermediate -> marts pattern
-- and give marts a stable contract.

select
    operation_at,
    operation_date,
    resource_id,
    user_id,
    credit_used,
    region,
    service_tier,
    operation_type,
    success,
    is_success_int,
    resource_type,
    invoice_id,
    currency

from {{ ref('stg_billing') }}