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
    dkSql['r2_2_rip_idf_ogda_thlb']="""
         CREATE TABLE r2_2_rip_idf_ogda_thlb AS
             SELECT 
               thlb.TSA_NAME,
               rpdfog.*,
               thlb.thlb_fact,
               ST_Area(ST_Intersection(rpdfog.geometry, thlb.geometry)) / 10000.0 AS AREA_HA,
               ST_Intersection(rpdfog.geometry, thlb.geometry) AS geometry
               
             FROM 
                 r2_2_rip_idf_ogda rpdfog
             JOIN 
                 thlb_tsa_qs thlb
             ON 
                 ST_Intersects(rpdfog.geometry, thlb.geometry); 
         
        -- Fix geometry field name
        ALTER TABLE r2_2_rip_idf_ogda_thlb DROP COLUMN IF EXISTS geometry;  
        ALTER TABLE r2_2_rip_idf_ogda_thlb RENAME COLUMN geometry_1 TO geometry; 
        
        -- Build a spatial index
        CREATE INDEX idx_r2_2_rip_idf_ogda_thlb ON r2_2_rip_idf_ogda_thlb USING RTREE (geometry);
                 """   
    '''      
    ########### MDWR intersection ##############  
    dkSql['r2_2_rip_idf_ogda_thlb_mdwr_fullattr']="""
        CREATE TABLE r2_2_rip_idf_ogda_thlb_mdwr_fullattr AS
            SELECT 
              thlb.*,
              mdr.LEGAL_FEAT_PROVID AS MDWR_OVERLAP,
              COALESCE(ST_Area(ST_Intersection(mdr.geometry, thlb.geometry)) / 10000.0, ST_Area(thlb.geometry) / 10000.0) AS AREA_HA,
              COALESCE(ST_Intersection(mdr.geometry, thlb.geometry), thlb.geometry) AS geometry
            FROM 
              r2_2_rip_idf_ogda_thlb_fullattr thlb
            LEFT JOIN 
              mdwr_kam mdr
            ON 
              ST_Intersects(mdr.geometry, thlb.geometry);

        
        -- Fix geometry field name
        ALTER TABLE r2_2_rip_idf_ogda_thlb_mdwr_fullattr DROP COLUMN IF EXISTS geometry;  
        ALTER TABLE r2_2_rip_idf_ogda_thlb_mdwr_fullattr RENAME COLUMN geometry_1 TO geometry; 
        
        -- Build a spatial index
        CREATE INDEX idx_r2_2_rip_idf_ogda_thlb_mdwr_fullattr ON r2_2_rip_idf_ogda_thlb_mdwr_fullattr USING RTREE (geometry);
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
        