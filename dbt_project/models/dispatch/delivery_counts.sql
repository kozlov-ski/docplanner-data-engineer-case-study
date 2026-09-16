{{ config(
    contract={'enforced': true},
    meta={'owner': 'dispatch', 'freshness_class': 'critical'}
) }}

-- Full rebuild keeps late-arriving events correct at interview-slice scale.
select
    date_add('minute', -mod(minute(occurred_at), 5),
             date_trunc('minute', occurred_at)) as window_start,
    count(*) as delivered_events
from {{ ref('stg_nomly__deliveries') }}
group by 1
