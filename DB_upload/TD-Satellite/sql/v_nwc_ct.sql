-- td_satellite.v_nwc_ct source

CREATE OR REPLACE VIEW td_satellite.v_nwc_ct
AS SELECT ns.title,
    ns.nominal_product_time AS "timestamp",
    ns.tz AS timezone,
    ns.site_name,
    ns.distance_site_grid_point_km,
    ns.ct,
    ns.ct_cumuliform,
    ns.ct_multilayer,
    ns.ct_status_flag,
    ns.ct_conditions AS ct_quality
   FROM td_satellite.nwc_satellite ns
  ORDER BY ns.site_name;