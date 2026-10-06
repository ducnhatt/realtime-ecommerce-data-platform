{{ config(order_by=['date']) }}

with bounds as (
    select
        min(review_date) as min_date,
        max(review_date) as max_date
    from {{ ref('stg_ecommerce_events_current') }}
)

select
    addDays(min_date, day_offset) as date,
    toUInt16(toYear(date)) as year,
    toUInt8(toQuarter(date)) as quarter,
    toUInt8(toMonth(date)) as month,
    toUInt8(toISOWeek(date)) as week_of_year,
    toUInt8(toDayOfWeek(date)) as day_of_week,
    cast(toDayOfWeek(date) in (6, 7), 'Bool') as is_weekend
from bounds
array join range(toUInt32(dateDiff('day', min_date, max_date) + 1)) as day_offset
