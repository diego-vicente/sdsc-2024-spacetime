-- Define interest areas based on what we have previously found
CREATE OR REPLACE TABLE
  `sdsc-london-2024.spacetime.london_collisions_wrap_up`
CLUSTER BY geom
AS (
  SELECT
    ST_UNION_AGG(`carto-un.carto.H3_BOUNDARY`(hotspot.index)) AS geom,
    CASE
      WHEN hotspot.classification = 'strengthening hotspot'  AND hotspot.tau_p < 0.05
      THEN 'Hotspot with an upward trend in accidents'
      WHEN lmi.quad = 'HL' AND lmi.p_value < 0.05
      THEN 'High concentration in the area surrounded by low concentration of accidents'
      WHEN clusters.cluster = '#2'
      THEN 'Same behavior as main hot spot in the city center'
      WHEN hotspot.classification LIKE 'fluctuating%' AND hotspot.tau_p < 0.05
      THEN 'Has been both a significant cold spot and hot spot'
    END AS reason
    FROM
      `sdsc-london-2024.spacetime.london_collisions_weekly_h3_quads` lmi
    FULL OUTER JOIN
      `sdsc-london-2024.spacetime.london_collisions_weekly_h3_clusters` clusters
      ON lmi.h3 = clusters.index
    FULL OUTER JOIN
      `sdsc-london-2024.spacetime.london_collisions_hotspot_classification` hotspot
      ON lmi.h3 = hotspot.index
    GROUP BY
      reason
    HAVING
      reason IS NOT NULL
)
