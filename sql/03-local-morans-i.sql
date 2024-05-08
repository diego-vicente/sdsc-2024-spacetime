-- Compute the Local Moran's I using the Analytics Toolbox
CALL `carto-un`.carto.LOCAL_MORANS_I_H3_TABLE(
  '''
    SELECT h3, SUM(n_collisions) AS n_collisions
    FROM `sdsc-london-2024.spacetime.london_collisions_weekly_h3`
    GROUP BY h3
  ''',
  'sdsc-london-2024.spacetime.london_collisions_weekly_h3_lmi',
  'h3',
  'n_collisions',
  2,
  'inverse',
  85
);

-- Filter and format the results for presenting
CREATE OR REPLACE TABLE
  `sdsc-london-2024.spacetime.london_collisions_weekly_h3_quads`
CLUSTER BY h3
AS (
  SELECT
    index AS h3,
    value AS spatial_autocorrelation,
    psim AS p_value,
    CASE
      WHEN quad = 1 THEN 'HH'
      WHEN quad = 2 THEN 'LL'
      WHEN quad = 3 THEN 'LH'
      WHEN quad = 4 THEN 'HL'
    END AS quad
  FROM
    `sdsc-london-2024.spacetime.london_collisions_weekly_h3_lmi`
  WHERE
    psim < 0.05
)

