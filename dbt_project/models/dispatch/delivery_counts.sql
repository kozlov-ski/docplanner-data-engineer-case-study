{{ config(
    contract={'enforced': true},
    meta={'owner': 'dispatch', 'freshness_class': 'critical'}
) }}

-- Full rebuild keeps late-arriving events correct at interview-slice scale.
select
    date_bin(
        interval '5 minutes',
        occurred_at,
        timestamptz '2000-01-01 00:00:00+00'
    ) as window_start,
    count(*) as delivered_events
from {{ ref('stg_nomly__deliveries') }}
group by 1
