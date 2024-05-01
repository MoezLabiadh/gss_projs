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
            

def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df



def load_dck_sql():
    dkSql= {}

    dkSql['poly_thlb_mature']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          thmt.INCLFACT,
          thmt.CONTCLAS,
          ROUND(ST_Area(ST_Intersection(thmt.geometry, poly.geometry))/10000,4)  AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(thmt.geometry, poly.geometry))* thmt.INCLFACT)/10000,4) AS THLB_MATURE_HA

        FROM 
          draft_fisher_polys poly  
              JOIN thlb_tsr2_mature thmt 
                  ON ST_Intersects(thmt.geometry, poly.geometry)
                    """

    dkSql['poly_uwr']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          uwg.UWR_NUMBER,
          uwg.TIMBER_HARVEST_CODE,
          thmt.INCLFACT,
          thmt.CONTCLAS, 
          ROUND(ST_Area(ST_Intersection(ST_Intersection(poly.geometry, uwg.geometry), thmt.geometry))/10000, 4) AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(ST_Intersection(poly.geometry, uwg.geometry), thmt.geometry))) * thmt.INCLFACT/10000, 4) AS UWR_THLB_MATURE_HA
          
        FROM 
            draft_fisher_polys AS poly
            JOIN 
                uwg ON ST_Intersects(poly.geometry, uwg.geometry)
            JOIN 
                thlb_tsr2_mature AS thmt ON ST_Intersects(poly.geometry, thmt.geometry) 
                    AND ST_Intersects(uwg.geometry, thmt.geometry)
                    
        WHERE
            uwg.UWR_NUMBER IN ('u-7-022', 'u-7-020', 'u-7-013', 'u-7-011');
                    """

    dkSql['poly_vqo']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          vqo.REC_EVQO_CODE,
          vqo.SCENIC_AREA_IND,
          thmt.INCLFACT,
          thmt.CONTCLAS, 
          ROUND(ST_Area(ST_Intersection(ST_Intersection(poly.geometry, vqo.geometry), thmt.geometry))/10000, 4) AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(ST_Intersection(poly.geometry, vqo.geometry), thmt.geometry))) * thmt.INCLFACT/10000, 4) AS VQO_THLB_MATURE_HA
          
        FROM 
            draft_fisher_polys AS poly
            JOIN 
                vqo ON ST_Intersects(poly.geometry, vqo.geometry)
            JOIN 
                thlb_tsr2_mature AS thmt ON ST_Intersects(poly.geometry, thmt.geometry) 
                    AND ST_Intersects(vqo.geometry, thmt.geometry);
                    """

    dkSql['poly_cofa']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          cofa.NON_LEGAL_OGMA_PROVID,
          thmt.INCLFACT,
          thmt.CONTCLAS, 
          ROUND(ST_Area(ST_Intersection(ST_Intersection(poly.geometry, cofa.geometry), thmt.geometry))/10000, 4) AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(ST_Intersection(poly.geometry, cofa.geometry), thmt.geometry))) * thmt.INCLFACT/10000, 4) AS VQO_THLB_MATURE_HA
          
        FROM 
            draft_fisher_polys AS poly
            JOIN 
                cofa ON ST_Intersects(poly.geometry, cofa.geometry)
            JOIN 
                thlb_tsr2_mature AS thmt ON ST_Intersects(poly.geometry, thmt.geometry) 
                    AND ST_Intersects(cofa.geometry, thmt.geometry);
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
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
    gdb= os.path.join(wks, 'inputs', 'data.gdb')
    
    print ('Connect to databases')    
    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn
    

    try:
        print ('Run Queries')
        dksql= load_dck_sql()
        q_rslt= run_duckdb_queries (dckCnx, dksql) 
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Duckdb.disconnect_db()

    print('Compute results')
    f_rslt= {}
    for k, v in q_rslt.items():
        if k== 'poly_thlb_mature':
            grpCols= ['DISTRICT', 'POLYGON_ID', 'REVISED', 'POLYGON_HA']
            sumCol= [col for col in v.columns if 'THLB_MATURE' in col][0]
            
            df= v.groupby(grpCols)[sumCol].sum().reset_index()
            
            df.sort_values(by=grpCols, inplace= True)
            
            f_rslt['SUMMARY '+k]= df
            
        elif k== 'poly_uwr':
            grpCols= ['POLYGON_ID', 'UWR_NUMBER', 'TIMBER_HARVEST_CODE']
            sumCol= [col for col in v.columns if 'THLB_MATURE' in col][0]

            df= v.groupby(grpCols)[sumCol].sum().reset_index()
            
            df.sort_values(by=grpCols, inplace= True)
            
            f_rslt['SUMMARY '+k]= df
            
        else:
            grpCols= ['POLYGON_ID']
            sumCol= [col for col in v.columns if 'THLB_MATURE' in col][0]
            
            df= v.groupby(grpCols)[sumCol].sum().reset_index()
            
            df.sort_values(by=grpCols, inplace= True)
            
            f_rslt['SUMMARY '+k]= df
            
            
        
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  