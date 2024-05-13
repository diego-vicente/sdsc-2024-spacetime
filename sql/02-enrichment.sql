-- Define the base grid
CREATE OR REPLACE TABLE
  `sdsc-london-2024.spacetime.london_h3_8`
CLUSTER BY h3
AS (
  SELECT
    h3
  FROM
    UNNEST (
      `carto-un`.carto.H3_POLYFILL(
        (
          SELECT geom
          FROM `sdsc-london-2024.spacetime.london_boundary`
        ),
        8
      )
    ) AS h3
);


-- Simple enrichment of LSOA population to H3
CALL `carto-un`.carto.ENRICH_GRID(
  'h3',
  'SELECT * FROM `sdsc-london-2024.spacetime.london_h3_8`',
  'h3',
  'SELECT * FROM `sdsc-london-2024.spacetime.lsoa_reference_2021`',
  'geom',
  [('population', 'sum')],
  ['`sdsc-london-2024.spacetime.london_lsoa_enrich_population_h3`']
);


-- Raw enrichment to relate H3 cells to LSOAs
CALL `carto-un`.carto.ENRICH_GRID_RAW(
  'h3',
  'SELECT * FROM `sdsc-london-2024.spacetime.london_h3_8`',
  'h3',
  'SELECT * FROM `sdsc-london-2024.spacetime.lsoa_reference_2021`',
  'geom',
  ['id', 'population', 'road_length'],
  ['`sdsc-london-2024.spacetime.london_lsoa_enrich_raw_h3`']
);


-- Use the results of the previous procedure to enrich keeping the time index
CREATE OR REPLACE TABLE
  `sdsc-london-2024.spacetime.london_collisions_weekly_h3`
CLUSTER BY week, h3
AS (
  WITH
    geo_pairs AS (
      SELECT 
        h3,
        enrichment.id AS lsoa_code,
        enrichment.__carto_total AS total_area,
        enrichment.__carto_intersection AS intersection,
      FROM 
        `sdsc-london-2024.spacetime.london_lsoa_enrich_raw_h3`,
        UNNEST (__carto_enrichment) AS enrichment
    )
  SELECT
    h3,
    week,
    SUM(n_collisions * (intersection / total_area)) AS n_collisions,
  FROM
    `sdsc-london-2024.spacetime.london_collisions_weekly`
  LEFT JOIN
    geo_pairs
    USING (lsoa_code)
  GROUP BY
    h3, week
  HAVING
    h3 IS NOT NULL
);

-- To display the results in Builder:
--
-- SELECT 
--   *
-- FROM
--   `sdsc-london-2024.spacetime.london_collisions_weekly_h3`
-- WHERE 
--   week BETWEEN DATE_TRUNC({{date_from}}, WEEK) AND DATE_TRUNC({{date_to}}, WEEK)
