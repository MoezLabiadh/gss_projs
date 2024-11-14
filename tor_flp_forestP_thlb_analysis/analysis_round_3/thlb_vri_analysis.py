import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import duckdb
import pandas as pd

class DuckDBConnector:
    def __init__(self, db=':memory:'):
        self.db = db
        self.conn = None
    
    def connect_to_db(self):
        """Connects to a DuckDB database and installs spatial extension."""
        self.conn = duckdb.connect(self.db)
        self.conn.install_extension('spatial')
        self.conn.load_extension('spatial')
        return self.conn
    
    def disconnect_db(self):
        """Disconnects from the DuckDB database."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            

def load_dck_sql():
    dkSql= {}
    '''
    dkSql['r3_vri_thlb']="""
         CREATE TABLE r3_vri_thlb AS
             SELECT 
               thlb.TSA_NAME,
               thlb.thlb_fact,
               vri.BEC_ZONE_CODE,
               vri.BEC_SUBZONE,
               vri.PROJ_AGE_1,
               vri.LIVE_STAND_VOLUME_125,
               ST_Area(ST_Intersection(thlb.geometry, vri.geometry)) / 10000.0 AS AREA_HA,
               ST_Intersection(thlb.geometry, vri.geometry) AS geometry
               
             FROM 
                 thlb_tsa_qs thlb
             JOIN 
                vri 
             ON 
                 ST_Intersects(thlb.geometry, vri.geometry); 

        -- Build a spatial index
        CREATE INDEX idx_r3_vri_thlb ON r3_vri_thlb USING RTREE (geometry);
                 """   

    dkSql['r3_rip_vri_thlb']="""
         CREATE TABLE r3_rip_vri_thlb AS
             SELECT 

               thlb.TSA_NAME,
               thlb.thlb_fact,
               thlb.BEC_ZONE_CODE,
               thlb.BEC_SUBZONE,
               thlb.PROJ_AGE_1,
               thlb.LIVE_STAND_VOLUME_125,
               rip.OVERLAP_TYPE,
               ST_Area(ST_Intersection(thlb.geometry, rip.geometry)) / 10000.0 AS AREA_HA,
               ST_Intersection(thlb.geometry, rip.geometry) AS geometry
               
             FROM 
                 r3_vri_thlb thlb
             JOIN 
                r2_2_rip_thlb_mdwr_fullattr rip 
             ON 
                 ST_Intersects(thlb.geometry, rip.geometry); 

        -- Build a spatial index
        CREATE INDEX idx_r3_rip_vri_thlb ON r3_rip_vri_thlb USING RTREE (geometry);
                 """ 
   
                 
   
    '''

    dkSql['r3_idf_vri_thlb']="""
         CREATE TABLE r3_idf_vri_thlb AS
             SELECT 
               thlb.TSA_NAME,
               thlb.thlb_fact,
               thlb.BEC_ZONE_CODE,
               thlb.BEC_SUBZONE,
               thlb.PROJ_AGE_1,
               thlb.LIVE_STAND_VOLUME_125,
               idf.OVERLAP_TYPE,
               idf.MDWR_OVERLAP,
               ST_Area(ST_Intersection(idf.geometry, thlb.geometry)) / 10000.0 AS AREA_HA,
               ST_Intersection(idf.geometry, thlb.geometry) AS geometry
               
             FROM 
                 r2_2_idf_thlb_mdwr_fullattr idf 
             JOIN 
                 r3_vri_thlb thlb
                
             ON 
                 ST_Intersects(idf.geometry, thlb.geometry); 

        -- Build a spatial index
        CREATE INDEX idx_r3_idf_vri_thlb ON r3_idf_vri_thlb USING RTREE (geometry);
                     """                         

                 
    return dkSql


def run_duckdb_queries (dckCnx, dict_sqls):
    """Run duckdb queries """
    results= {}
    counter = 1
    for k, v in dict_sqls.items():
        print(f'..running query {counter} of {len(dict_sqls)}: {k}')
        results[k]= dckCnx.execute(v).df()
        
        counter+= 1
        
    return results

    

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'

    print ('Connecting to databases')    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'tor_flp_thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn 
    dckCnx.execute("SET GLOBAL pandas_analyze_sample=1000000")
    dckCnx.execute("PRAGMA max_temp_directory_size='100GiB'")
    
    try:
        print ('Run Queries')
        dksql= load_dck_sql()
        run_duckdb_queries (dckCnx, dksql) 

        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        Duckdb.disconnect_db()
        
        
finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
        