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
            


def add_buffer_width(dckCnx, wks, tables):
    specs_file= os.path.join(wks, 'inputs', 'riparian_buffers.xlsx')
    df= pd.read_excel(specs_file)
    df=df[['Riparian Class Match', 'Buffer width @ 100% Retention']]
    
    for table in tables:
        print (f'...working on {table}')
        dckCnx.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS buffer_width DOUBLE")
        
        dckCnx.execute(f"""
            UPDATE {table} 
            SET buffer_width = (
                SELECT df."Buffer width @ 100% Retention"
                FROM df
                WHERE df."Riparian Class Match" = {table}.riparian_class
            )
            """)
    
    dckCnx.commit()
    
 
def create_buffers (dckCnx, wks, tables):
    for table in tables:
        buffered_table = f"{table}_buffered"
        print (f'...working on {buffered_table}')
        dckCnx.execute(f"""
            CREATE TABLE {buffered_table} AS
            SELECT *,
                   ST_Buffer(geometry, buffer_width) AS buffered_geometry
            FROM {table}
            """)
    
    dckCnx.commit()
    

def merge_riparian (dckCnx):
    dckCnx.execute("""
        -- Step 1: Merge the tables and union the geometries
        CREATE TABLE merged_geometry AS
            SELECT buffered_geometry AS geometry
            FROM (
                SELECT buffered_geometry FROM rivers_buffered
                UNION ALL
                SELECT buffered_geometry FROM lakes_buffered
                UNION ALL
                SELECT buffered_geometry FROM wetlands_buffered
                UNION ALL
                SELECT buffered_geometry FROM streams_buffered
            ) AS combined_geometries;
        
        -- Step 2: Dissolve any overlaps
        CREATE TABLE dissolved_geometry AS
            SELECT ST_Union(geometry) AS geometry
            FROM merged_geometry;
                   
        """)
        
        
     
if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'

    print ('Connecting to databases')    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'tor_flp_thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn 
    dckCnx.execute("SET GLOBAL pandas_analyze_sample=1000000")
    
    
    try:
        tables = ['rivers', 'lakes', 'wetlands', 'streams']
        
        print ('Add buffer column')
        #add_buffer_width(dckCnx, wks)
        
        print ('Create buffered features')
        #create_buffers (dckCnx, wks, tables)
        
        print ('Merge and dissolve riparian features')
        merge_riparian (dckCnx)

        
    
       
       
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        Duckdb.disconnect_db()
        
finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
        