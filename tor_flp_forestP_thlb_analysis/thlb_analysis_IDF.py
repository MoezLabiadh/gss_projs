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
    dkSql['idf_thlb']="""
    --Create a table for IDF THLB calulcation
        CREATE TABLE idf_thlb AS
            SELECT 
              idf.*,
              thlb.thlb_fact,
              ST_Intersection(idf.geometry, thlb.geometry) AS geometry
              
            FROM 
              idf
                  JOIN thlb ON ST_Intersects(idf.geometry, thlb.geometry)
                  
            WHERE idf.PROJ_AGE_1 >= 100;
            
     -- Fix geometry field name
     ALTER TABLE idf_thlb DROP COLUMN IF EXISTS GEOMETRY;  
     ALTER TABLE idf_thlb RENAME COLUMN geometry_1 TO geometry;     
                    """
    '''          
    dkSql['idf_thlb_aoi']="""
    --Create a table for IDF THLB AOIs
        CREATE TABLE idf_thlb_aoi AS
            SELECT 
              aoi.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
              aoi.WATERSHED_GROUP_NAME AS PLAN_AREA_NAME,
              thlb.*,
              ROUND(ST_Area(
                      ST_Intersection(aoi.geometry, thlb.geometry))/10000, 4) AS AREA_HA,
              ROUND(ST_Area(
                      ST_Intersection(aoi.geometry, thlb.geometry))/10000 * thlb.thlb_fact, 4) AS THLB_AREA_HA,
              ST_Intersection(aoi.geometry, thlb.geometry) AS geometry
 
            FROM 
              tsa_plan_areas AS aoi
                  JOIN idf_thlb thlb ON ST_Intersects(aoi.geometry, thlb.geometry);
    
     -- Fix geometry field name
     ALTER TABLE idf_thlb_aoi DROP COLUMN IF EXISTS geometry;  
     ALTER TABLE idf_thlb_aoi RENAME COLUMN geometry_1 TO geometry; 
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
    
    try:
        print ('Run Queries')
        dksql= load_dck_sql()
        #run_duckdb_queries (dckCnx, dksql) 
        
        
        print ('Compute stats')
        df= dckCnx.execute("""SELECT*  EXCLUDE geometry FROM idf_thlb_aoi""").df()
        
        #df['TIMBER_VOLUME_M3']= df['LIVE_STAND_VOLUME_125'] * df['THLB_AREA_HA']
        df['IMPACT_FACT']= 0.5
        df['THLB_IMPACT_HA']= df['THLB_AREA_HA'] * df['IMPACT_FACT']
        #df['VOLUME_IMPACT_M3']= df['TIMBER_VOLUME_M3'] * df['IMPACT_FACT']
        
        df_sum = df.groupby(['TSA_NAME', 'PLAN_AREA_NAME'])[['AREA_HA', 'THLB_AREA_HA', 'THLB_IMPACT_HA']].sum().reset_index()
        
        df_sum_tsa = df.groupby(['TSA_NAME'])[['AREA_HA', 'THLB_AREA_HA','THLB_IMPACT_HA']].sum().reset_index()
       
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        Duckdb.disconnect_db()
        
finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
        