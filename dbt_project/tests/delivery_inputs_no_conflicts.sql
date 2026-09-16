-- Attach to the input model: dbt build must stop before replacing downstream counts.
select event_id
from {{ ref('stg_nomly__delivery_inputs') }}
where event_id is not null
group by event_id
having count(distinct payload) > 1
