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
    ########### IDF THLB intersection ##############               
    dkSql['idf_thlb']="""
        CREATE TABLE idf_thlb AS
            SELECT 
              idf.*,
              thlb.thlb_fact,
              ST_Intersection(idf.geometry, thlb.geometry) AS geometry
              
            FROM 
              idf
                  JOIN 
              thlb ON ST_Intersects(idf.geometry, thlb.geometry);
            
     -- Fix geometry field name
     ALTER TABLE idf_thlb DROP COLUMN IF EXISTS GEOMETRY;  
     ALTER TABLE idf_thlb RENAME COLUMN geometry_1 TO geometry;     
                    """

    dkSql['idf_thlb_tsa']="""
        CREATE TABLE idf_thlb_tsa AS
            SELECT 
              tsa.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
              thlb.*,
              ST_Intersection(tsa.geometry, thlb.geometry) AS geometry
              
            FROM 
                tsa_planArea tsa
            JOIN 
                idf_thlb thlb
            ON 
                ST_Intersects(tsa.geometry, thlb.geometry); 
        
        -- Fix geometry field name
        ALTER TABLE idf_thlb_tsa DROP COLUMN IF EXISTS geometry;  
        ALTER TABLE idf_thlb_tsa RENAME COLUMN geometry_1 TO geometry;  
                    """
    ''' 
    
    dkSql['idf_thlb_tsa_mdwr']="""
        CREATE TABLE idf_thlb_tsa_mdwr AS
            SELECT 
              thlb.*,
              mdr.LEGAL_FEAT_PROVID,
              COALESCE(ST_Area(ST_Intersection(mdr.geometry, thlb.geometry)) / 10000.0, ST_Area(thlb.geometry) / 10000.0) AS AREA_HA,
              COALESCE(ST_Intersection(mdr.geometry, thlb.geometry), thlb.geometry) AS geometry
            FROM 
              idf_thlb_tsa thlb
            LEFT JOIN 
              mdwr_kam mdr
            ON 
              ST_Intersects(mdr.geometry, thlb.geometry);

        
        -- Fix geometry field name
        ALTER TABLE idf_thlb_tsa_mdwr DROP COLUMN IF EXISTS geometry;  
        ALTER TABLE idf_thlb_tsa_mdwr RENAME COLUMN geometry_1 TO geometry;  
                    """



    '''
    ########### TSA/Plan Area total THLB calulcation ##############  
    dkSql['tsa_full_thlb']="""
        CREATE TABLE tsa_full_thlb AS
            SELECT 
              tsa.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
              thlb.thlb_fact,
              ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0 AS AREA_HA,
              (ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0) * thlb.thlb_fact AS THLB_AREA,
              ST_Intersection(tsa.geometry, thlb.geometry) AS geometry
              
            FROM 
              tsa
                  JOIN 
              thlb ON ST_Intersects(tsa.geometry, thlb.geometry);  
                    """
                    
                    
    dkSql['tsa_planA_thlb']="""
        CREATE TABLE tsa_planA_thlb AS
            SELECT 
              tsa.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
              thlb.thlb_fact,
              ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0 AS AREA_HA,
              (ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0) * thlb.thlb_fact AS THLB_AREA,
              ST_Intersection(tsa.geometry, thlb.geometry) AS geometry
              
            FROM 
              tsa_planArea tsa
                  JOIN 
              thlb ON ST_Intersects(tsa.geometry, thlb.geometry);  
                    """   
                    
    ########### Okanagan Scenarios ##############
    dkSql['idf_thlb_okanagan_scenario1']="""
        CREATE TABLE idf_thlb_okanagan_scenario1 AS
            SELECT 
                tsa.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
                thlb.POLYGON_ID,
                thlb.BEC_ZONE_CODE,
                thlb.BEC_SUBZONE,
                thlb.QUAD_DIAM_125,
                thlb.PROJ_AGE_1,
                thlb.LIVE_STAND_VOLUME_125,
                thlb.thlb_fact,
                
                ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0 AS AREA_HA,  -- Area in hectares
                (ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0) * thlb.thlb_fact AS THLB_AREA,
                
                CASE
                    WHEN thlb.BEC_SUBZONE NOT IN ('mm', 'mw', 'dk', 'dm', 'mw') THEN 0.5  -- 50% reduction factor
                    WHEN thlb.BEC_SUBZONE IN ('dk', 'dm', 'mw') THEN 0.14  -- 14% reduction factor
                    ELSE 0  -- No reduction
                END AS REDUCTION_FACTOR,
                
                ((ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0) * thlb.thlb_fact) * 
                (1 - CASE
                         WHEN thlb.BEC_SUBZONE NOT IN ('mm', 'mw', 'dk', 'dm', 'mw') THEN 0.5
                         WHEN thlb.BEC_SUBZONE IN ('dk', 'dm', 'mw') THEN 0.14
                         ELSE 0
                 END) AS THLB_IMPACT,  -- THLB area after reduction
                    
                ST_Intersection(tsa.geometry, thlb.geometry) AS geometry  -- geometry column
                
            FROM 
                tsa_planArea tsa
            JOIN 
                idf_thlb thlb
            ON 
                ST_Intersects(tsa.geometry, thlb.geometry)  -- Spatial join condition
            WHERE 
                tsa.TSA_NUMBER_DESCRIPTION = 'Okanagan TSA'
                AND thlb.PROJ_AGE_1 >= 100;
                    """                               
        
                
    dkSql['idf_thlb_okanagan_scenario2']="""
        CREATE TABLE idf_thlb_okanagan_scenario2 AS
            SELECT 
                tsa.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
                thlb.POLYGON_ID,
                thlb.BEC_ZONE_CODE,
                thlb.BEC_SUBZONE,
                thlb.QUAD_DIAM_125,
                thlb.PROJ_AGE_1,
                thlb.LIVE_STAND_VOLUME_125,
                thlb.thlb_fact,
                
                ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0 AS AREA_HA,  -- Area in hectares
                (ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0) * thlb.thlb_fact AS THLB_AREA,
                
                CASE
                    WHEN thlb.BEC_SUBZONE NOT IN ('mm', 'mw', 'dk', 'dm', 'mw') THEN 0.5  -- 50% reduction factor
                    WHEN thlb.BEC_SUBZONE IN ('dk', 'dm', 'mw') THEN 0.14  -- 14% reduction factor
                    ELSE 0  -- No reduction
                END AS REDUCTION_FACTOR,
                
                ((ST_Area(ST_Intersection(tsa.geometry, thlb.geometry)) / 10000.0) * thlb.thlb_fact) * 
                (1 - CASE
                         WHEN thlb.BEC_SUBZONE NOT IN ('mm', 'mw', 'dk', 'dm', 'mw') THEN 0.5
                         WHEN thlb.BEC_SUBZONE IN ('dk', 'dm', 'mw') THEN 0.14
                         ELSE 0
                 END) AS THLB_IMPACT,  -- THLB area after reduction
                    
                ST_Intersection(tsa.geometry, thlb.geometry) AS geometry  -- geometry column
                
            FROM 
                tsa_planArea tsa
            JOIN 
                idf_thlb thlb
            ON 
                ST_Intersects(tsa.geometry, thlb.geometry)  -- Spatial join condition
            WHERE 
                tsa.TSA_NUMBER_DESCRIPTION = 'Okanagan TSA'
                AND thlb.PROJ_AGE_1 >= 60;
                    """   
    '''             
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
        