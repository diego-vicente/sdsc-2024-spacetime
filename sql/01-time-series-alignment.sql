-- Create a table with the cartesian product of all LSOA codes and weeks
CREATE OR REPLACE TABLE
  `sdsc-london-2024.spacetime.london_collisions_all_indexes`
AS (
  WITH
    all_lsoa AS (
      SELECT 
        DISTINCT id AS lsoa_code
      FROM 
        `sdsc-london-2024.spacetime.lsoa_reference_2021`
    ),
    all_weeks AS (
      SELECT DISTINCT
        DATE_TRUNC(date, WEEK) AS week
      FROM UNNEST (
        GENERATE_DATE_ARRAY(
          DATE '2021-01-03',
          DATE '2022-12-31',
          INTERVAL 7 DAY
        )
      ) AS date
    )
  SELECT lsoa_code, week
  FROM all_lsoa, all_weeks
);


-- Create a table with the weekly number of collisions per LSOA
CREATE OR REPLACE TABLE
  `sdsc-london-2024.spacetime.london_collisions_weekly`
AS (
  SELECT
    lsoa_code, week,
    COUNT(DISTINCT collision_id) AS n_collisions
  FROM
    `sdsc-london-2024.spacetime.london_collisions_all_indexes` all_idx
  LEFT JOIN
    `sdsc-london-2024.spacetime.london_collisions` coll
  ON
    coll.collision_location_lsoa_code = all_idx.lsoa_code
    AND DATE_TRUNC(coll.date, WEEK) = all_idx.week
  GROUP BY
    lsoa_code,
    week
);


-- To display the results in Builder:
--
-- SELECT
--   lsoa_code,
--   ANY_VALUE(geom) AS geom,
--   SUM(n_collisions) AS n_collisions
-- FROM
--   `sdsc-london-2024.spacetime.london_collisions_weekly` coll
-- LEFT JOIN
--   `sdsc-london-2024.spacetime.lsoa_reference_2021` lsoa
-- ON
--   coll.lsoa_code = lsoa.id
-- WHERE 
--   week BETWEEN {{date_from}} AND {{date_to}}
-- GROUP BY 
--   lsoa_code
