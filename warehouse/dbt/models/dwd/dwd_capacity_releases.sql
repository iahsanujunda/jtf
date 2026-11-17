{#
  METI's quarterly releases of FIT/FIP capacity and when each became known: one row per release,
  once both its tables (A by prefecture, B by municipality) are held. Before the source's capture
  start, the history rule: the end of the latest announcement date that a copy of the summary page
  taken before capture start showed, never before the files' Last-Modified, and a release never
  announced at the end of its files' day + 16 days. From capture start, the fetch that first held
  both files. A file fetched again (a re-upload) is a new version of the release's values, never
  a new date for the release: the dates come from each table's first copy.
  Designed in notebooks/03_solar_capacity.ipynb, section 2, and notebooks/04_ods_to_dwd.ipynb,
  section 6.
#}

with capture_start as (
    select min(known_at) as t
    from {{ ref('dwd_source_captures') }}
    where source = 'meti_fit_capacity'
),

-- A re-parse appends a copy's rows again under a new parser_version: read each copy as last parsed.
notices as (
    select *
    from {{ source('ods', 'meti_fit_notices') }}
    qualify ingested_at = max(ingested_at) over (partition by source_file)
),

-- The rule reads only copies of the page taken before capture start, so a later copy can't move
-- it. A copy was taken at the archive's time for an archived copy (source_time), at our fetch for
-- our own, whose source_time is empty.
announced as (
    select ref_date, max(announced) as announced
    from notices
    where coalesce(source_time, fetched_at) < (select t from capture_start)
    group by ref_date
),

files as (
    select
        last_day(parse_date('%Y%m', regexp_extract(filename, r'^[AB]_(?:pref|city)(\d{6})\.xlsx$')))
            as ref_date,
        ods_table,
        fetched_at,
        parse_timestamp('%a, %d %b %Y %H:%M:%S %z', last_modified) as last_modified
    from {{ source('ods', 'raw_files') }}
    where ods_table in ('meti_fit_capacity_pref', 'meti_fit_capacity_city') and status = 'parsed'
),

-- Each table's first copy: its Last-Modified dates the release, its fetch is when we first held it.
first_copies as (
    select
        ref_date,
        ods_table,
        min(fetched_at) as first_fetched,
        any_value(last_modified having min fetched_at) as last_modified
    from files
    group by ref_date, ods_table
),

held as (
    select
        ref_date,
        max(if(ods_table = 'meti_fit_capacity_pref', last_modified, null)) as a_last_modified,
        max(if(ods_table = 'meti_fit_capacity_city', last_modified, null)) as b_last_modified,
        max(last_modified) as last_modified,
        max(first_fetched) as first_held
    from first_copies
    group by ref_date
    having count(*) = 2
),

ruled as (
    select
        held.*,
        announced.announced,
        timestamp(date_add(announced.announced, interval 1 day), 'Asia/Tokyo') as announced_end,
        if(
            announced.announced is not null,
            greatest(
                timestamp(date_add(announced.announced, interval 1 day), 'Asia/Tokyo'),
                held.last_modified
            ),
            timestamp(
                date_add(date(held.last_modified, 'Asia/Tokyo'), interval 17 day), 'Asia/Tokyo'
            )
        ) as rule_time
    from held
    left join announced using (ref_date)
),

-- As for every source, the rule dates only what falls before capture start (with none yet, nothing).
dated as (
    select
        *,
        coalesce(rule_time < (select t from capture_start), false) as by_rule
    from ruled
)

select
    ref_date,
    if(by_rule, rule_time, first_held) as known_at,
    -- A release is one fact, not a series of versions: nothing ends it.
    cast(null as timestamp) as known_until,
    if(by_rule, 'rule', 'captured') as basis,
    case
        when not by_rule then null
        when announced is null then 'margin'
        when rule_time = announced_end then 'announced'
        else 'last_modified'
    end as rule,
    announced,
    a_last_modified,
    b_last_modified,
    first_held
from dated
