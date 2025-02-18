{{ config(
    materialized = "view"
) }}


with coins_list as (
    select 
        id, 
        symbol, 
        name, 
        row_update_date, 
        row_number() over (partition by id order by row_update_date) as row_num
    from CRYPTO_DATA.PUBLIC.CRYPTO_COINS_LIST
)

select 
    id, 
    symbol, 
    name, 
    row_update_date
from coins_list where row_num = 1
