import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt


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
            

def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def read_local_data(loc_dict, data_dict):
    counter = 1
    for k, v in loc_dict.items():
        print(f'....table {counter} of {len(loc_dict)}: {k}')
        df= esri_to_gdf (v)
        df['GEOMETRY']= df['geometry'].apply(lambda x: wkt.dumps(x, output_dimension=2))
        df['GEOMETRY'] = df['GEOMETRY'].astype(str)
        
        df = df.drop(columns=['geometry'])
        
        data_dict[k]= df
        
        counter+=1
        
    return data_dict   


def add_data_to_duckdb(dckCnx, data_dict):
    counter = 1
    for k, v in data_dict.items():
        print(f'..table {counter} of {len(data_dict)}: {k}')
        dck_tab_list = dckCnx.execute('SHOW TABLES').df()['name'].to_list()

        if k in dck_tab_list:
            dck_row_count = dckCnx.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0]
            dck_col_nams = dckCnx.execute(f"""SELECT column_name 
                                             FROM INFORMATION_SCHEMA.COLUMNS 
                                             WHERE table_name = '{k}'""").df()['column_name'].to_list()

            if (dck_row_count != len(v)) or (set(list(v.columns)) != set(dck_col_nams)):
                print(f'....import to Duckdb ({v.shape[0]} rows)')
                chunk_size = 10000
                total_chunks = (len(v) + chunk_size - 1) // chunk_size
                
                # Process the initial chunk and create the table
                initial_chunk = v.iloc[0:chunk_size]
                print(f'.......processing chunk 1 of {total_chunks}')
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {k} AS
                    SELECT * EXCLUDE GEOMETRY, ST_GeomFromText(GEOMETRY) AS GEOMETRY
                    FROM initial_chunk;
                """
                dckCnx.execute(create_table_query)
                
                # Process and append the rest of the chunks
                for i in range(chunk_size, len(v), chunk_size):
                    chunk = v.iloc[i:i + chunk_size]
                    chunk_number = (i // chunk_size) + 1
                    print(f'.......processing chunk {chunk_number} of {total_chunks}')
                    
                    insert_query = f"""
                    INSERT INTO {k}
                        SELECT * EXCLUDE GEOMETRY, ST_GeomFromText(GEOMETRY) AS GEOMETRY
                        FROM chunk;
                    """
                    dckCnx.execute(insert_query)
            else:
                print('....data already in db: skip importing')
                pass

        else:
            print(f'....import to Duckdb ({v.shape[0]} rows)')
            chunk_size = 10000
            total_chunks = (len(v) + chunk_size - 1) // chunk_size
            
            # Process the initial chunk and create the table
            initial_chunk = v.iloc[0:chunk_size]
            print(f'.......processing chunk 1 of {total_chunks}')
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {k} AS
                SELECT * EXCLUDE GEOMETRY, ST_GeomFromText(GEOMETRY) AS GEOMETRY
                FROM initial_chunk;
            """
            dckCnx.execute(create_table_query)
            
            # Process and append the rest of the chunks
            for i in range(chunk_size, len(v), chunk_size):
                chunk = v.iloc[i:i + chunk_size]
                chunk_number = (i // chunk_size) + 1
                print(f'.......processing chunk {chunk_number} of {total_chunks}')
                
                insert_query = f"""
                INSERT INTO {k}
                    SELECT * EXCLUDE GEOMETRY, ST_GeomFromText(GEOMETRY) AS GEOMETRY
                    FROM chunk;
                """
                dckCnx.execute(insert_query)
                
        #Add a spatial index to the table
        print(f'....creating a spatial RTREE index')
        dckCnx.execute(f'CREATE INDEX idx_2_{k} ON {k} USING RTREE (GEOMETRY);')

        counter += 1


            
if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'
    gdb= os.path.join(wks, 'inputs', 'data.gdb')
    
    print ('Connecting to databases')    
    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'tor_flp_thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn 
    dckCnx.execute("SET GLOBAL pandas_analyze_sample=1000000")
    

    try:
        print ('\nReading input data')

        loc_dict= {}
        #loc_dict['tsa']= os.path.join(gdb, 'tsa')
        #loc_dict['tsa_planArea']= os.path.join(gdb, 'tsa_planArea')
        #loc_dict['mdwr_kam']= os.path.join(gdb, 'mdwr_kam')
        #loc_dict['mdwr_kam']= os.path.join(gdb, 'mdwr_kam')
        #loc_dict['ogda']= os.path.join(gdb, 'ogda')
        #loc_dict['thlb']= os.path.join(gdb, 'thlb_tsas')
        #loc_dict['tsa_qs']= os.path.join(gdb, 'tsa_qs')
        #loc_dict['riparian_buffers_fbp']= os.path.join(gdb, 'merged_dissolved_rip_fbp_modified')
        
        #round 2 of analysis

        #loc_dict['r2_2_rip_ogda']= os.path.join(gdb, 'r2_2_rip_ogda')
        #loc_dict['r2_2_rip_idf']= os.path.join(gdb, 'r2_2_rip_idf')
        #loc_dict['r2_2_rip_idf_ogda_thlb']= os.path.join(gdb, 'r2_2_rip_idf_ogda_thlb')
        #loc_dict['r2_2_KAM_rip_idf_thlb_mdwr']= os.path.join(gdb, 'r2_2_KAM_rip_idf_thlb_mdwr_fn_v2')
        loc_dict['r2_2_KAM_rip_idf_ogda_thlb_mdwr']= os.path.join(gdb, 'r2_2_KAM__rip_idf_ogda_thlb_mdwr_fn')
        
        data_dict= {}
        
        print('\n..reading from Local files')
        data_dict= read_local_data(loc_dict, data_dict)
        
        print('\nWriting data to duckdb')
        add_data_to_duckdb(dckCnx, data_dict)
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Duckdb.disconnect_db()
    
        
    
        finish_t = timeit.default_timer() #finish time
        t_sec = round(finish_t-start_t)
        mins = int (t_sec/60)
        secs = int (t_sec%60)
        print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  