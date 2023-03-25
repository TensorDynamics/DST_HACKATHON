-- td_wrf.v_wrf_data source

create or replace
view td_wrf.v_wrf_data
as with wrf_view as (
select
	base_table.site_name,
	t2.latitude,
	t2.longitude,
	t2.type as site_type,
	base_table.times as "timestamp",
	base_table.tz as timezone,
	base_table.t2 - 273.15::double precision as temp_c,
	sqrt((base_table.u10 * base_table.u10 + base_table.v10 * base_table.v10)::double precision) as wind_speed_10m_mps,
	(180::numeric / 3.14159)::double precision * atan((base_table.v10 / base_table.u10)::double precision) as wind_direction_in_deg,
	row_number() over (partition by base_table.times,
	base_table.site_name
order by
	base_table.init_date desc) as rn,
	base_table.swdown as swdown_wpm2
from
	td_wrf.td_wrf_stg base_table
left join (
	select
		cs.site_name,
		cs.state,
		cs.capacity,
		cs.type,
		cs.latitude,
		cs.longitude,
		cs.row_id,
		cs.log_ts
	from
		configs.site_config cs) t2 on
	base_table.site_name = t2.site_name
order by
	base_table.site_name,
	base_table.times
        )
 select
	wrf_view.site_name,
	wrf_view.latitude,
	wrf_view.longitude,
	wrf_view.site_type,
	wrf_view."timestamp",
	wrf_view.timezone,
	wrf_view.temp_c,
	wrf_view.wind_speed_10m_mps,
	wrf_view.wind_direction_in_deg,
	wrf_view.swdown_wpm2
from
	wrf_view
where
	wrf_view.rn = 1;