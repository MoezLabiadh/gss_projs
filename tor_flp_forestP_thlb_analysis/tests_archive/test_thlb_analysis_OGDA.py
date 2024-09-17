import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkt

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
    
    dkSql['ogda_no_overlap']="""
    CREATE TABLE ogda_no_overlap AS
        WITH aggregated_overlaps AS (
            SELECT ogda.geometry AS ogda_geom,
                   ST_Union_Agg(idf.geometry) AS idf_union,
                   ST_Union_Agg(rivers.geometry) AS rivers_union
            FROM ogda
            LEFT JOIN idf ON ST_Intersects(ogda.geometry, idf.geometry)
            LEFT JOIN rivers ON ST_Intersects(ogda.geometry, rivers.geometry)
            GROUP BY ogda.geometry
        )
        SELECT ogda.*,
               ST_Difference(
                   ST_Difference(ogda.geometry, COALESCE(agg.idf_union, ST_GeomFromText('POLYGON EMPTY'))),
                   COALESCE(agg.rivers_union, ST_GeomFromText('POLYGON EMPTY'))
               ) AS new_geometry
        FROM ogda
        LEFT JOIN aggregated_overlaps agg ON ogda.geometry = agg.ogda_geom;
    """
    
    '''
    dkSql['ogda_thlb']="""
    --Create a table for OGDA THLB calulcation
        CREATE TABLE ogda_thlb AS
            SELECT 
              ogda.OGSR_TOF_SYSID,
              ogda.BGC_LABEL,
              thlb.thlb_fact,
              ST_Intersection(ogda.geometry, thlb.geometry) AS geometry
              
            FROM 
              ogda
                  JOIN thlb ON ST_Intersects(ogda.geometry, thlb.geometry);
            
                    """

                    
    dkSql['ogda_thlb_aoi']="""
    --Create a table for OGDA THLB AOIs
        CREATE TABLE ogda_thlb_aoi AS
            SELECT 
              aoi.TSA_NUMBER_DESCRIPTION AS TSA_NAME,
              aoi.WATERSHED_GROUP_NAME AS PLAN_AREA_NAME,
              thlb.OGSR_TOF_SYSID,
              thlb.BGC_LABEL,
              thlb.thlb_fact,
              ROUND(ST_Area(
                      ST_Intersection(aoi.geometry, thlb.geometry))/10000, 4) AS AREA_HA,
              ROUND(ST_Area(
                      ST_Intersection(aoi.geometry, thlb.geometry))/10000 * thlb.thlb_fact, 4) AS THLB_AREA_HA,
              ST_Intersection(aoi.geometry, thlb.geometry) AS geometry
 
            FROM 
              tsa_plan_areas aoi
                  JOIN ogda_thlb thlb ON ST_Intersects(aoi.geometry, thlb.geometry);
                  
        ALTER TABLE ogda_no_overlap DROP COLUMN IF EXISTS GEOMETRY;
        ALTER TABLE ogda_no_overlap RENAME COLUMN new_geometry TO geometry;
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
        #results= run_duckdb_queries (dckCnx, dksql) 
        
        
        
        
        
        df = dckCnx.execute("""SELECT * EXCLUDE geometry, 
                        ST_AsText(geometry) AS wkt_geom 
                    FROM ogda_no_overlap
                            """).fetch_df()
            
        df['geometry'] = df['wkt_geom'].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        gdf.drop(columns=['wkt_geom'], inplace=True)
        
        gdf.crs = "EPSG:3005"
        
        #gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom if geom.geom_type == 'Polygon' else geom.convex_hull)
        gdf = gdf.explode(index_parts=False)
        
        
        print ('export to featureclass')
        gdb= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis\inputs\data.gdb'
        gdf.to_file(gdb,layer="test_ogda_nooverlaps", driver="OpenFileGDB")
        
        
        
        '''
        print ('Compute stats')
        df= dckCnx.execute("""SELECT*  EXCLUDE geometry FROM ogda_thlb_aoi""").df()
        
        df['IMPACT_FACT']= 1
        df['THLB_IMPACT_HA']= df['THLB_AREA_HA'] * df['IMPACT_FACT']
        
        df_sum = df.groupby(['TSA_NAME', 'PLAN_AREA_NAME'])[['AREA_HA', 'THLB_AREA_HA', 'THLB_IMPACT_HA']].sum().reset_index()
        '''
       
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        Duckdb.disconnect_db()
        
finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
        