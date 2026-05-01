with source as (
    select * from {{ source('raw', 'wallet_records') }}
),

renamed as (
    select
        id                              as record_id,
        account_id,
        category_id,
        amount_value                    as amount_cop,
        amount_currency                 as currency_code,
        record_type,
        note,
        cast(record_date as timestamp)  as record_date,
        cast(created_at as timestamp)   as created_at,
        cast(updated_at as timestamp)   as updated_at,
        cast(_loaded_at as timestamp)   as loaded_at
    from source
)

select * from renamed
