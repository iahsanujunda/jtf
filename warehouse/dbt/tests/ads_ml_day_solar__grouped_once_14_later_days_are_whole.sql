-- A day's sky is set exactly when the 14 days after it are all in the table, and not before: a
-- day grouped sooner would move between groups as later days arrived, and one never grouped
-- would drop out of every result by kind of day.
select d.day, d.sky, count(n.day) as later_days
from {{ ref('ads_ml_day_solar') }} as d
left join {{ ref('ads_ml_day_solar') }} as n
    on n.day between date_add(d.day, interval 1 day) and date_add(d.day, interval 14 day)
group by d.day, d.sky
having (count(n.day) = 14) != (d.sky is not null)
