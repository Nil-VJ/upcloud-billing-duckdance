-- One row per (operation_date, service_tier, operation_type, currency).
-- Use case: finance breakdown by tier and operation type.

select
    operation_date,
    service_tier,
    operation_type,
    currency,
    sum(credit_used)            as total_credits,
    avg(credit_used)            as avg_credit_per_operation,
    count(*)                    as operation_count

from {{ ref('int_billing_enriched') }}

group by operation_date, service_tier, operation_type, currency