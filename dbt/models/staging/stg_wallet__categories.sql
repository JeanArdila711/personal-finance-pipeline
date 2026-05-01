with source as (
    select * from {{ source('raw', 'wallet_categories') }}
),

renamed as (
    select
        id                               as category_id,
        name                             as category_name,
        color,
        cast(custom_category as boolean) as is_custom,
        envelope_id,
        cast(created_at as timestamp)    as created_at,
        cast(updated_at as timestamp)    as updated_at,
        cast(_loaded_at as timestamp)    as loaded_at
    from source
)

select * from renamed
