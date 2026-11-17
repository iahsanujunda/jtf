{#
  Installed solar per capacity release and part of the TEPCO area, as versions, in MW. A release's
  first version is known when the release is (dwd_capacity_releases). A file of it fetched again
  with different figures is a new version, known at that fetch, and never before the release; the
  same figures fetched again are not. Copies fetched before the release was known (one table
  re-fetched while the other was still missing) count as one, the latest. Rebuilt in full every
  run: the figures it reads are dwd_capacity_area_files', a few hundred rows.
  Designed in notebooks/04_ods_to_dwd.ipynb, sections 6.6 to 6.8.
#}

with copies as (
    select
        f.ref_date,
        f.point,
        f.solar_mw,
        f.source_file,
        f.fetched_at,
        r.known_at as release_known_at,
        r.basis as release_basis,
        row_number() over (
            partition by f.ref_date, f.point order by f.fetched_at, f.source_file
        ) as copy
    from {{ ref('dwd_capacity_area_files') }} as f
    inner join {{ ref('dwd_capacity_releases') }} as r using (ref_date)
),

dated as (
    select
        *,
        if(copy = 1, release_known_at, greatest(fetched_at, release_known_at)) as known_at
    from copies
),

-- Copies dated at the same time, because they were fetched before the release was known: the
-- latest is what the release held when it became known.
at_known as (
    select *
    from dated
    qualify row_number() over (
        partition by ref_date, point, known_at order by fetched_at desc, source_file desc
    ) = 1
),

sightings as (
    select
        *,
        lag(solar_mw) over (partition by ref_date, point order by known_at) as previous_mw
    from at_known
),

versions as (
    select
        *,
        previous_mw is not null as correction
    from sightings
    where previous_mw is null or solar_mw != previous_mw
)

select
    ref_date,
    point,
    solar_mw,
    known_at,
    lead(known_at) over (partition by ref_date, point order by known_at) as known_until,
    if(correction, 'captured', release_basis) as basis,
    fetched_at as first_seen,
    source_file,
    correction
from versions
