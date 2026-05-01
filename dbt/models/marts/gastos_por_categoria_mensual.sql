with fact as (
    select * from {{ ref('fact_transactions') }}
),

categories as (
    select category_id, category_name from {{ ref('dim_categories') }}
)

select
    f.month,
    f.year,
    f.month_num,
    c.category_name,
    count(*)           as num_transacciones,
    sum(f.amount_cop)  as total_cop,
    avg(f.amount_cop)  as promedio_cop
from fact f
left join categories c using (category_id)
where f.record_type = 'expense'
group by 1, 2, 3, 4
order by 1 desc, total_cop asc
