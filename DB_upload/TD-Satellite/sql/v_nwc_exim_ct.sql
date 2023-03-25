-- td_satellite.v_nwc_exim_ct source

CREATE OR REPLACE VIEW td_satellite.v_nwc_exim_ct
AS WITH exim_ct_data AS (
         SELECT ns.title,
            ns.calc_nominal_product_time,
            ns.calc_date_created,
            ns.tz,
            ns.site_name,
            ns.distance_site_grid_point_km,
            ns.ct,
            ns.ct_status_flag,
            ns.exim_status_flag,
            ns.exim_conditions,
            ns.exim_quality,
            rank() OVER (PARTITION BY ns.site_name, ns.calc_nominal_product_time ORDER BY ns.calc_date_created DESC) AS rn
           FROM td_satellite.nwc_exim_ct ns
          ORDER BY ns.site_name, ns.calc_nominal_product_time
        )
 SELECT exim_ct_data.title,
    exim_ct_data.calc_nominal_product_time AS "timestamp",
    exim_ct_data.tz AS timezone,
    exim_ct_data.site_name,
    exim_ct_data.distance_site_grid_point_km,
    exim_ct_data.ct,
    exim_ct_data.ct_status_flag,
    exim_ct_data.exim_status_flag,
    exim_ct_data.exim_conditions,
    exim_ct_data.exim_quality
   FROM exim_ct_data
  WHERE exim_ct_data.rn = 1;