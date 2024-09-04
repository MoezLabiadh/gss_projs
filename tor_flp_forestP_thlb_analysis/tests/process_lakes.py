import timeit
import duckdb

start_t = timeit.default_timer() #start time 


db= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis\inputs\to_flp_thlb_analysis.db'
conn = duckdb.connect(db)

conn.install_extension('spatial')
conn.load_extension('spatial')

'''
print ('Populate CLASS column')

sql_cls="""
    ALTER TABLE lakes
    ADD COLUMN CLASS VARCHAR;
    
    UPDATE lakes
    SET CLASS = CASE
        WHEN AREA_HA > 1000 THEN 'L1-A'
        WHEN AREA_HA > 5 AND AREA_HA <= 1000 THEN 'L1-B'
        WHEN AREA_HA > 1 AND AREA_HA <= 5 THEN 'L2/L3'
        WHEN AREA_HA > 0.25 AND AREA_HA <= 1 THEN 'L4'
        ELSE 'NCL'
    END;
"""
conn.execute(sql_cls)



print ('Create a Lakes RRZ table')

sql_rrz="""
    CREATE TABLE rrz_lakes AS
        SELECT 
            WATERBODY_POLY_ID,
            FEATURE_CODE,
            GNIS_NAME_1,
            AREA_HA,
            CLASS,
            ST_Buffer(geometry, 10) AS geometry
        FROM 
            lakes
        WHERE 
            CLASS != 'NCL';
"""
conn.execute(sql_rrz)
'''

print ('Create a Lakes RMZ table')

sql_rmz="""
    CREATE TABLE rmz_lakes AS
        SELECT 
            WATERBODY_POLY_ID,
            FEATURE_CODE,
            GNIS_NAME_1,
            AREA_HA,
            CLASS,
            CASE 
                WHEN CLASS IN ('L1-A', 'L1-B') THEN ST_Buffer(geometry, 190)
                ELSE ST_Buffer(geometry, 90)
            END AS geometry
    FROM 
        rrz_lakes;
"""
conn.execute(sql_rmz)


conn.close()



finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds') 