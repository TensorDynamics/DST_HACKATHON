-- td_satellite.v_ct_ci_map source

CREATE OR REPLACE VIEW td_satellite.v_ct_ci_map
AS SELECT main.snapshot_date,
    main.site_name,
    main.ct,
    meta.flag_meaning,
    main.min_ci,
    main.max_ci,
    main.avg_ci,
    main.median_ci
   FROM ( SELECT t.ct,
            t.min_ci,
            t.max_ci,
            t.avg_ci,
            t.median_ci,
            t.snapshot_date,
            t.site_name
           FROM ( SELECT ct_ci_data.ct,
                    ct_ci_data.min_ci,
                    ct_ci_data.max_ci,
                    ct_ci_data.avg_ci,
                    ct_ci_data.median_ci,
                    ct_ci_data.site_name,
                    ct_ci_data.snapshot_date,
                    rank() OVER (ORDER BY ct_ci_data.snapshot_date DESC) AS rn
                   FROM td_satellite.ct_ci_data) t
          WHERE t.rn = 1
          ORDER BY t.site_name, t.ct) main
     LEFT JOIN ( SELECT cmi.flag_value,
            cmi.flag_meaning
           FROM td_satellite.ct_meta_info cmi
          WHERE cmi.variable = 'ct'::text) meta ON meta.flag_value::double precision = main.ct;