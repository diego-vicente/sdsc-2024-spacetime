-- Compute the space-time Getis-Ord using the Analytics Toolbox
CALL `carto-un`.carto.GETIS_ORD_SPACETIME_H3_TABLE(
  'sdsc-london-2024.spacetime.london_collisions_weekly_h3',
  'sdsc-london-2024.spacetime.london_collisions_weekly_h3_gi',
  'h3',
  'week',
  'n_collisions',
  3,
  'WEEK',
  1,
  'gaussian',
  'gaussian'
);


-- To display the results on Builder:
--
-- SELECT
--   index AS h3,
--   date AS week,
--   gi,
--   p_value
-- FROM `sdsc-london-2024.spacetime.london_collisions_weekly_h3_gi` 
-- WHERE
--   p_value < 0.05
--   AND date BETWEEN DATE_TRUNC({{date_from}}, WEEK) AND DATE_TRUNC({{date_to}}, WEEK)
