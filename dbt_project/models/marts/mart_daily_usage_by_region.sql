-- One row per (operation_date, region, currency).
-- Use case: regional capacity and revenue trends.

select
    operation_date,
    region,
    currency,
    sum(credit_used)            as total_credits,
    count(*)                    as operation_count,
    avg(is_success_int)         as success_rate

from {{ ref('int_billing_enriched') }}

group by operation_date, region, currency