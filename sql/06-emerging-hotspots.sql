-- Use a preview function to run the classification
CALL `cartodb-on-gcp-datascience.dvicente_at_carto.SPACETIME_HOTSPOTS_CLASSIFICATION`(
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
--   classification.classification AS classification
-- FROM 
--   `sdsc-london-2024.spacetime.london_collisions_hotspot_classification`
-- WHERE 
--   classification.classification <> 'undetected pattern'
--   AND classification.tau_p < 0.05
