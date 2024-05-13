-- Use a preview function to run the classification
CALL `sdsc-london-2024.preview_carto.SPACETIME_HOTSPOTS_CLASSIFICATION`(
  'sdsc-london-2024.spacetime.london_collisions_weekly_h3_gi',
  'sdsc-london-2024.spacetime.london_collisions_hotspot_classification',
  'index',
  'date',
  'gi',
  'p_value',
  '{"threshold": 0.05, "algorithm": "mmk"}'
);


-- To visualize the results in Builder:
--
-- SELECT 
--   index AS h3, 
--   classification AS classification
-- FROM 
--   `sdsc-london-2024.spacetime.london_collisions_hotspot_classification`
-- WHERE 
--   classification <> 'undetected pattern'
--   AND tau_p < 0.05
