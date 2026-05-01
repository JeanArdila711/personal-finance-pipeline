with accounts as (
    select * from {{ ref('stg_wallet__accounts') }}
)

select
    account_id,
    account_name,
    account_type,
    currency_code,
    initial_balance_cop,
    exclude_from_stats,
    record_count,
    created_at,
    updated_at
from accounts
