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

    dkSql['thlb_plan_areas']="""
    --Create a table for Gross THLB calulcation - THLB by plan area
        CREATE TABLE thlb_plan_areas AS
            SELECT 
              aoi.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
              aoi.WATERSHED_GROUP_NAME AS PLAN_AREA_NAME,
              thlb.thlb_fact,
              ROUND(ST_Area(
                      ST_Intersection(aoi.geometry, thlb.geometry))/10000, 4) AS AREA_HA,
              ROUND(ST_Area(
                      ST_Intersection(aoi.geometry, thlb.geometry))/10000 * thlb.thlb_fact, 4) AS GROSS_THLB_AREA_HA,
              
              --ST_Intersection(aoi.geometry, thlb.geometry) AS geometry
              
            FROM 
              tsa_plan_areas AS aoi
                  JOIN thlb ON ST_Intersects(aoi.geometry, thlb.geometry);    
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
        df= dckCnx.execute("""SELECT* FROM thlb_plan_areas""").df()
        
        
        df_sum = df.groupby(['TSA_NAME', 'PLAN_AREA_NAME'])[['AREA_HA', 'GROSS_THLB_AREA_HA']].sum().reset_index()
        
        df_sum_tsa_gross = df.groupby(['TSA_NAME'])[['AREA_HA', 'GROSS_THLB_AREA_HA']].sum().reset_index()
       
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        Duckdb.disconnect_db()
        
finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
        