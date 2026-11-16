{#
  CTEs ending in `position`: for every timestamp `ts` of the relation `times` and every point
  (`point`, `lat`, `lon`) of the relation `points`, the sun's geometric height in degrees
  (`elevation`) and Haurwitz clear-sky radiation in W/m² (`clear_sky_ghi`). Insert it into a WITH
  list:

      with times as (...), points as (...), {{ sun_position('times', 'points') }}, next as (...)

  The equations are NOAA's (declination and equation of time from Julian centuries since J2000;
  the hour angle from true solar time), refraction left out. Within 0.015° of NREL's SPA over
  249,183 samples (notebooks/05_clear_sky.ipynb, section 1). BigQuery's MOD takes no floats, so
  the wrap to one day is written with FLOOR.
#}
{% macro sun_position(times, points) -%}
{%- set r = '(acos(-1) / 180)' -%}
sun_centuries as (
    select ts, (unix_micros(ts) / 8.64e10 + 2440587.5 - 2451545.0) / 36525.0 as jc
    from {{ times }}
),

sun_orbit as (
    select
        ts,
        jc,
        280.46646 + jc * (36000.76983 + jc * 0.0003032) as mean_longitude,
        357.52911 + jc * (35999.05029 - 0.0001537 * jc) as mean_anomaly,
        0.016708634 - jc * (0.000042037 + 0.0000001267 * jc) as eccentricity,
        23 + (26 + (21.448 - jc * (46.815 + jc * (0.00059 - jc * 0.001813))) / 60) / 60
            + 0.00256 * cos({{ r }} * (125.04 - 1934.136 * jc)) as obliquity
    from sun_centuries
),

-- Declination (degrees) and the equation of time (minutes)
sun_declination as (
    select
        ts,
        asin(sin({{ r }} * obliquity) * sin({{ r }} * (
            mean_longitude
            + sin({{ r }} * mean_anomaly) * (1.914602 - jc * (0.004817 + 0.000014 * jc))
            + sin({{ r }} * 2 * mean_anomaly) * (0.019993 - 0.000101 * jc)
            + sin({{ r }} * 3 * mean_anomaly) * 0.000289
            - 0.00569 - 0.00478 * sin({{ r }} * (125.04 - 1934.136 * jc))
        ))) / {{ r }} as declination,
        4 / {{ r }} * (
            y * sin(2 * {{ r }} * mean_longitude)
            - 2 * eccentricity * sin({{ r }} * mean_anomaly)
            + 4 * eccentricity * y * sin({{ r }} * mean_anomaly) * cos(2 * {{ r }} * mean_longitude)
            - 0.5 * y * y * sin(4 * {{ r }} * mean_longitude)
            - 1.25 * eccentricity * eccentricity * sin(2 * {{ r }} * mean_anomaly)
        ) as equation_of_time
    from (select *, pow(tan({{ r }} * obliquity / 2), 2) as y from sun_orbit)
),

-- True solar time in minutes: the UTC clock, the equation of time, 4 minutes per degree east
sun_solar_time as (
    select
        p.point,
        p.lat,
        s.ts,
        s.declination,
        extract(hour from s.ts) * 60 + extract(minute from s.ts) + extract(second from s.ts) / 60
            + s.equation_of_time + 4 * p.lon as minutes
    from sun_declination as s
    cross join {{ points }} as p
),

sun_angles as (
    select
        point,
        ts,
        sin({{ r }} * lat) * sin({{ r }} * declination)
            + cos({{ r }} * lat) * cos({{ r }} * declination)
            -- the hour angle: 0 at solar noon, 15° per hour from it
            * cos({{ r }} * ((minutes - 1440 * floor(minutes / 1440)) / 4 - 180)) as cos_zenith
    from sun_solar_time
),

position as (
    select
        point,
        ts,
        asin(cos_zenith) / {{ r }} as elevation,
        if(cos_zenith > 0, 1098 * cos_zenith * exp(-0.059 / cos_zenith), 0) as clear_sky_ghi
    from sun_angles
)
{%- endmacro %}
