with records as (
    select * from {{ ref('stg_wallet__records') }}
),

accounts as (
    select account_id from {{ ref('dim_accounts') }}
),

categories as (
    select category_id from {{ ref('dim_categories') }}
)

select
    r.record_id,
    r.account_id,
    r.category_id,
    r.amount_cop,
    r.currency_code,
    r.record_type,
    r.note,
    r.record_date,
    date_trunc('month', r.record_date::timestamp)::date  as month,
    extract(year from r.record_date::timestamp)::integer  as year,
    extract(month from r.record_date::timestamp)::integer as month_num,
    r.created_at
from records r
left join accounts a using (account_id)
left join categories c using (category_id)
