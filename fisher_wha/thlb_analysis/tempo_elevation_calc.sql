WITH polygon_areas AS (
  SELECT
    p.id,
    s.slope_class,
    ST_Area(ST_Intersection(p.geometry, s.geometry)) AS area
  FROM
    training_polygons p
    CROSS JOIN slope_classes s
  WHERE
    ST_Intersects(p.geometry, s.geometry)
)
SELECT
  id,
  MEDIAN(
    CASE
      WHEN SUM(area) OVER (PARTITION BY id) = 0 THEN NULL
      ELSE SQRT(area) * slope_class / SUM(SQRT(area)) OVER (PARTITION BY id)
    END
  ) AS median_elevation
FROM
  polygon_areas
GROUP BY
  id;
  
  
  
  The WITH clause creates a Common Table Expression (CTE) named polygon_areas that calculates the intersection area between each training polygon and each slope class. This CTE returns a result set with columns for polygon ID, slope class, and the area of the intersection.
The main query selects the polygon ID and calculates the median elevation using a window function.
Inside the MEDIAN function, we have a CASE statement that handles two scenarios:

If the sum of areas for a polygon is zero (meaning no intersection with any slope class), it returns NULL.
Otherwise, it calculates a weighted average of the slope classes using the square root of the area as the weight. The square root is used to balance the contribution of large and small areas.


The SUM(SQRT(area)) OVER (PARTITION BY id) calculates the sum of the square roots of areas for each polygon, which is used as the normalization factor in the weighted average calculation.
The GROUP BY clause groups the results by the polygon ID to ensure we get one row per polygon.

Note that this query assumes the following:

The training_polygons table has a column named id (or you can modify the query to use a different column name) and a geometry column named geometry.
The slope_classes table has a column named slope_class (or you can modify the query to use a different column name) and a geometry column named geometry.
The slope_class column represents the elevation value associated with each slope class.