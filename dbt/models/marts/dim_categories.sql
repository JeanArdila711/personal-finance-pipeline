with categories as (
    select * from {{ ref('stg_wallet__categories') }}
)

select
    category_id,
    category_name,
    color,
    is_custom,
    envelope_id,
    created_at,
    updated_at
from categories
