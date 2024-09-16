import timeit
import duckdb

start_t = timeit.default_timer() #start time 


db= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis\inputs\to_flp_thlb_analysis.db'
conn = duckdb.connect(db)

conn.install_extension('spatial')
conn.load_extension('spatial')



'''
print ('Add TSA table')
sql_tsa="""
    DROP TABLE IF EXISTS wetlands_merritt;
    CREATE TABLE wetlands_merritt AS
    SELECT w.*
    FROM wetlands w
    JOIN tsa t
    ON ST_Intersects(w.geometry, t.geometry)
    WHERE t.TSA_NAME = 'Merritt TSA';
    """

conn.execute(sql_tsa)


tsa= 'wetlands_merritt'

print (f'Add COMPLEX values: {tsa}')

sql_comp= F"""
    -- Add the COMPLEX column
    ALTER TABLE {tsa} DROP COLUMN IF EXISTS COMPLEX;
    ALTER TABLE {tsa} ADD COLUMN COMPLEX STRING DEFAULT 'No';
    
    -- Update the COMPLEX column based on the spatial conditions
    WITH candidate_pairs AS (
        SELECT
            a.WATERBODY_POLY_ID AS id_a,
            b.WATERBODY_POLY_ID AS id_b,
            a.geometry AS geom_a,
            b.geometry AS geom_b,
            ST_Distance(a.geometry, b.geometry) AS distance,
            a.AREA_HA AS area_a,
            b.AREA_HA AS area_b
        FROM
            {tsa} a,
            {tsa} b
        WHERE
            a.WATERBODY_POLY_ID < b.WATERBODY_POLY_ID
    )
    UPDATE {tsa}
    SET COMPLEX = 'Yes'
    FROM candidate_pairs
    WHERE 
        {tsa}.WATERBODY_POLY_ID IN (candidate_pairs.id_a, candidate_pairs.id_b)
        AND (
            -- Both wetlands are <5 ha and separated by 60 m or less
            (candidate_pairs.area_a < 5 AND candidate_pairs.area_b < 5 AND candidate_pairs.distance <= 60)
            OR
            -- One wetland is <5 ha and the other >5 ha, and separated by 80 m or less
            ((candidate_pairs.area_a < 5 AND candidate_pairs.area_b > 5) OR 
             (candidate_pairs.area_a > 5 AND candidate_pairs.area_b < 5))
            AND candidate_pairs.distance <= 80
            OR
            -- Both wetlands are >5 ha and separated by 100 m or less
            (candidate_pairs.area_a > 5 AND candidate_pairs.area_b > 5 AND candidate_pairs.distance <= 100)
        );

"""


conn.execute(sql_comp)

sql_comb="""
    CREATE TABLE wetlands_combined AS
        SELECT * FROM wetlands_merritt
        UNION ALL
        SELECT * FROM wetlands_kamloops
        UNION ALL
        SELECT * FROM wetlands_okanagan
        UNION ALL
        SELECT * FROM wetlands_100_mile_house;
"""
conn.execute(sql_comb)



print ('Populate CLASS column')

sql_cls="""
    ALTER TABLE wetlands
    ADD COLUMN CLASS VARCHAR;
    
    UPDATE wetlands
    SET CLASS = CASE
        WHEN COMPLEX = 'Yes' THEN 'W5'
        WHEN COMPLEX = 'No' AND AREA_HA > 5 THEN 'W1'
        WHEN COMPLEX = 'No' AND AREA_HA > 1 AND AREA_HA <= 5 THEN 'W2/W3'
        WHEN COMPLEX = 'No' AND AREA_HA > 0.25 AND AREA_HA <= 1 THEN 'W4'
        ELSE 'NCW'
    END;
"""
conn.execute(sql_cls)
'''


print ('Create a Wetlands RRZ table')

sql_rrz="""
    CREATE TABLE rrz_wetlands AS
        SELECT 
            WATERBODY_POLY_ID,
            FEATURE_CODE,
            GNIS_NAME_1,
            AREA_HA,
            CLASS,
            ST_Buffer(geometry, 10) AS geometry
        FROM 
            wetlands
        WHERE 
            CLASS != 'NCW';
"""
conn.execute(sql_rrz)


print ('Create a Wetlands RMZ table')

sql_rmz="""
    CREATE TABLE rmz_wetlands AS
        SELECT 
            WATERBODY_POLY_ID,
            FEATURE_CODE,
            GNIS_NAME_1,
            AREA_HA,
            CLASS,
            CASE 
                WHEN CLASS = 'W4' THEN ST_Buffer(geometry, 40)
                ELSE ST_Buffer(geometry, 90)
            END AS geometry
    FROM 
        rrz_wetlands;
"""
conn.execute(sql_rmz)

conn.close()



finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds') 