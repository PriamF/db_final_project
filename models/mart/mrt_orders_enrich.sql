{{ config(
    materialized = 'table'
}}

select *

from source{{'local_bike','orders' }}