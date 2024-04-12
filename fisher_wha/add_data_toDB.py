import os
import timeit
import duckdb
import geopandas as gpd
from shapely import wkb


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


def gdf_to_duckdb (conn, gdf, table_name):
    """Insert data from a gdf into a duckdb table """
    gdf_wkb= gdf.copy()
    gdf_wkb['geometry']= gdf_wkb['geometry'].apply(lambda x: wkb.dumps(x, output_dimension=2))
    create_table_query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
              SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS geometry
              FROM gdf_wkb;
    """
    conn.execute(create_table_query)
    


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
     
    print ('Connecting to duckdb')
    Duckdb= DuckDBConnector(db='proj_db')
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
    
    try:
        print ('\nAdding tables to duckdb')
        gdb= os.path.join(wks, 'inputs', 'data.gdb')
        tables= ['VRI', 'THLB']
        for t in tables:
            print (f'...adding {t}')
            gdf= gpd.read_file(filename= gdb, layer= t)
            gdf_to_duckdb (dckCnx, gdf, t)
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Duckdb.disconnect_db()

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds') 