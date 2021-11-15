with orders as (
    select * from {{ ref('stg_orders') }}
)
,payments as (
    select * from {{ ref('stg_payments') }}
)
,final as (
    select
        orders.order_id
        ,customer_id
        ,sum(case when payments.status = 'success' then payments.amount else 0 end) as amount
    from orders
    left join payments
        on payments.order_id = orders.order_id
    group by 1,2
)

select * from final