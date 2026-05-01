with source as (
    select * from {{ source('raw', 'wallet_accounts') }}
),

renamed as (
    select
        id                                          as account_id,
        name                                        as account_name,
        account_type,
        cast(archived as boolean)                   as is_archived,
        color,
        initial_balance_value                       as initial_balance_cop,
        initial_balance_currency                    as currency_code,
        cast(exclude_from_stats as boolean)         as exclude_from_stats,
        record_count,
        cast(created_at as timestamp)               as created_at,
        cast(updated_at as timestamp)               as updated_at,
        cast(_loaded_at as timestamp)               as loaded_at
    from source
)

select * from renamed
where not is_archived
