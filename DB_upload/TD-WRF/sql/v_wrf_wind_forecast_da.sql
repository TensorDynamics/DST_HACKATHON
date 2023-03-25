-- td_wrf.v_wrf_wind_forecast_da source

create or replace
view td_wrf.v_wrf_wind_forecast_da
as with wind_forecast_view as (
select
	forecast_table.site_name,
	forecast_table."timestamp",
	forecast_table.ws,
	forecast_table."power(kw)",
	forecast_table."temp(c)",
	forecast_table.snapshot_date,
	row_number() over (partition by forecast_table."timestamp",
	forecast_table.site_name
order by
	forecast_table.snapshot_date desc) as rn
from
	td_wrf.wrf_wind_forecast_da forecast_table
        )
 select
	wind_forecast_view.site_name,
	wind_forecast_view."timestamp",
	wind_forecast_view.ws,
	wind_forecast_view."power(kw)",
	wind_forecast_view."temp(c)",
	wind_forecast_view.snapshot_date
from
	wind_forecast_view
where
	wind_forecast_view.rn = 1
order by
	wind_forecast_view.site_name,
	wind_forecast_view."timestamp";