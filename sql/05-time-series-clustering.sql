-- Compute the time series clusters using a preview function
CALL `sdsc-london-2024.preview_carto.TIME_SERIES_CLUSTERING`(
  '''
    SELECT * FROM `sdsc-london-2024.spacetime.london_collisions_weekly_h3_gi`
    QUALIFY PERCENTILE_CONT(p_value, 0.6) OVER (PARTITION BY index) < 0.05
  ''',
  'index',
  'date',
  'gi',
  'sdsc-london-2024.spacetime.london_collisions_weekly_h3_clusters',
  JSON '{ "method": "profile", "n_clusters": 4 }'
);


-- To display the results in Builder:
--
-- SELECT 
--   h3,
--   week,
--   cluster,
--   n_collisions,
-- FROM 
--   `sdsc-london-2024.spacetime.london_collisions_weekly_h3_clusters` tsc
-- INNER JOIN
--   `sdsc-london-2024.spacetime.london_collisions_weekly_h3` coll
-- ON 
--   tsc.index = coll.h3
