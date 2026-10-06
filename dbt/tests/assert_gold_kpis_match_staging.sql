with staging_kpis as (
    select
        count() as event_count,
        sum(quantity) as quantity_sum,
        sum(total_amount) as gross_item_value,
        avg(overall_rating) as avg_rating
    from {{ ref('stg_ecommerce_events_current') }}
),

gold_kpis as (
    select
        count() as event_count,
        sum(quantity) as quantity_sum,
        sum(total_amount) as gross_item_value,
        avg(overall_rating) as avg_rating
    from {{ ref('fact_ecommerce_event') }}
)

select 1
from staging_kpis
cross join gold_kpis
where staging_kpis.event_count != gold_kpis.event_count
   or staging_kpis.quantity_sum != gold_kpis.quantity_sum
   or staging_kpis.gross_item_value != gold_kpis.gross_item_value
   or staging_kpis.avg_rating != gold_kpis.avg_rating
