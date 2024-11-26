{{config(
    materialized='table'
)

}}

WITH order_table as (

select 
order_id, 
customer_id, 
order_date, 
store_id, 
staff_id

from {{ source('local_bike', 'orders') }}
),

order_sales as (

select
order_id,
sum(list_price*(1-discount)*quantity) as order_sales,
sum(quantity) as quantity

from {{source("local_bike","order_items")}}
group by order_id

),

store_detail as (

select
store_id,
store_name,
city as store_city,
state as store_state

from {{source("local_bike","stores")}}
),

customer_detail as (

select
customer_id,
CONCAT(first_name, ' ', last_name) as customer_name,
city as customer_city,
state as customer_state,

from {{source("local_bike","customers")}}

),

staff_detail as (

select
staff_id,
CONCAT(first_name, ' ', last_name) as staff_name,

from {{source("local_bike","staffs")}}

)

select

o.order_id,
o.customer_id,
o.order_date,
s.order_sales,
s.quantity,
o.store_id,
d.store_name,
d.store_city,
d.store_state,
c.customer_name,
c.customer_city,
c.customer_state,
o.staff_id,
sd.staff_name,


from order_table as o
left join order_sales as s on o.order_id = s.order_id
left join store_detail as d on o.store_id = d.store_id
left join customer_detail as c on o.customer_id = c.customer_id
left join staff_detail as sd on o.staff_id = sd.staff_id





