import warnings
warnings.simplefilter(action='ignore')

import os
import json
import timeit
import cx_Oracle
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt


class OracleConnector:
    def __init__(self, dbname='BCGW'):
        self.dbname = dbname
        self.cnxinfo = self.get_db_cnxinfo()

    def get_db_cnxinfo(self):
        """ Retrieves db connection params from the config file"""
        with open(r'H:\config\db_config.json', 'r') as file:
            data = json.load(file)
        
        if self.dbname in data:
            return data[self.dbname]
        
        raise KeyError(f"Database '{self.dbname}' not found.")
    
    def connect_to_db(self):
        """ Connects to Oracle DB and create a cursor"""
        try:
            self.connection = cx_Oracle.connect(self.cnxinfo['username'], 
                                                self.cnxinfo['password'], 
                                                self.cnxinfo['hostname'], 
                                                encoding="UTF-8")
            self.cursor = self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception(f'..Connection failed: {e}')

    def disconnect_db(self):
        """Close the Oracle connection and cursor"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("....Disconnected from the database")


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


def df_2_gdf (df, geometry_col, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df[geometry_col] = df[geometry_col].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df[geometry_col])
    
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = "EPSG:" + str(crs)
    
    del gdf[geometry_col]
    
    return gdf

    
def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def load_Orc_sql():
    orSql= {}


               
    orSql['lakes_2'] = """
        SELECT
            WATERBODY_POLY_ID,
            FEATURE_CODE,
            GNIS_NAME_1,
            AREA_HA,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY
             
        FROM
            WHSE_BASEMAPPING.FWA_LAKES_POLY
        WHERE
            SDO_RELATE(GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 'mask=ANYINTERACT') = 'TRUE' 
               """                

    '''  
    

    orSql['streams'] = """
        SELECT
            LINEAR_FEATURE_ID,
            FEATURE_CODE,
            FEATURE_SOURCE,
            DOWNSTREAM_ROUTE_MEASURE,
            GNIS_NAME,
            LEFT_RIGHT_TRIBUTARY,
            STREAM_ORDER,
            STREAM_MAGNITUDE,
            WATERBODY_KEY,
            SDO_UTIL.TO_WKTGEOMETRY(SDO_CS.MAKE_2D(GEOMETRY)) AS GEOMETRY
        FROM
            WHSE_BASEMAPPING.FWA_STREAM_NETWORKS_SP
        WHERE
            SDO_RELATE(GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 'mask=ANYINTERACT') = 'TRUE' 
               """ 


               
    orSql['tsa'] = """
         SELECT
             TSA_NUMBER_DESCRIPTION AS TSA_NAME,
             SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY 
         FROM
         WHSE_ADMIN_BOUNDARIES.FADM_TSA
         WHERE
             TSB_NUMBER IS NULL 
             AND RETIREMENT_DATE IS NULL 
             AND TSA_NUMBER_DESCRIPTION IN ('100 Mile House TSA', 'Kamloops TSA', 
                                            'Merritt TSA', 'Okanagan TSA')
                """        


               
               
    orSql['wetlands'] = """
        SELECT
            WATERBODY_POLY_ID,
            FEATURE_CODE,
            GNIS_NAME_1,
            AREA_HA,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY
             
        FROM
            WHSE_BASEMAPPING.FWA_WETLANDS_POLY
        WHERE
            SDO_RELATE(GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 'mask=ANYINTERACT') = 'TRUE' 
               """  
               

    orSql['bec_mature'] = """
        SELECT 
            bec.ZONE,
            bec.SUBZONE,
            POLYGON_ID,
            BEC_ZONE_CODE,
            BEC_SUBZONE,
            BEC_VARIANT,
            SITE_INDEX,
            LINE_3_TREE_SPECIES,
            SPECIES_CD_1,
            SPECIES_PCT_1,
            SPECIES_CD_2,
            SPECIES_PCT_2,
            SPECIES_CD_3,
            SPECIES_PCT_3,
            PROJ_AGE_1,
            PROJ_AGE_CLASS_CD_1
            PROJ_AGE_2,
            PROJ_AGE_CLASS_CD_2,
            LIVE_STAND_VOLUME_125,
            SDO_UTIL.TO_WKTGEOMETRY(SDO_GEOM.SDO_INTERSECTION(vri.GEOMETRY, bec.GEOMETRY, 0.005)) AS GEOMETRY
        FROM 
            WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY vri
        JOIN 
            WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY bec
        ON 
            SDO_RELATE(vri.GEOMETRY, bec.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
        WHERE 
            vri.PROJ_AGE_1 >= 80
            AND bec.ZONE = 'IDF'
            AND bec.SUBZONE IN ('dc', 'dh', 'dk', 'dm', 'dw', 
                                'xc', 'xh', 'xk', 'xm', 'xw', 'xx') 
            AND SDO_RELATE(bec.GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 'mask=ANYINTERACT') = 'TRUE' 
                    """
                    
             
    orSql['bec'] = """
        SELECT
            bec.ZONE,
            bec.SUBZONE,
            bec.VARIANT,
            bec.PHASE,
            bec.NATURAL_DISTURBANCE,
            bec.MAP_LABEL,
            bec.BGC_LABEL,
            SDO_UTIL.TO_WKTGEOMETRY(bec.GEOMETRY) AS GEOMETRY 
            
        FROM
            WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY bec
        WHERE
            bec.ZONE= 'IDF'
            AND bec.SUBZONE IN ('dc', 'dh', 'dk', 'dm', 'dw', 
                                'xc', 'xh', 'xk', 'xm', 'xw', 'xx')
            AND SDO_RELATE(GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 'mask=ANYINTERACT') = 'TRUE'
                    """
                    
    orSql['vri'] = """
        SELECT
            POLYGON_ID,
            BEC_ZONE_CODE,
            BEC_SUBZONE,
            BEC_VARIANT,
            SITE_INDEX,
            LINE_3_TREE_SPECIES,
            SPECIES_CD_1,
            SPECIES_PCT_1,
            SPECIES_CD_2,
            SPECIES_PCT_2,
            SPECIES_CD_3,
            SPECIES_PCT_3,
            PROJ_AGE_1,
            PROJ_AGE_CLASS_CD_1
            PROJ_AGE_2,
            PROJ_AGE_CLASS_CD_2,
            LIVE_STAND_VOLUME_125,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY 
        FROM
            WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY
        WHERE
            SDO_RELATE(GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 'mask=ANYINTERACT') = 'TRUE'
                    """   
        '''
    return orSql
            

def read_oracle_data (orcCnx, orcCur, dict_sqls, wkb_aoi, srid, data_dict):
    counter = 1
    for k, v in dict_sqls.items():
        print(f'....table {counter} of {len(dict_sqls)}: {k}')
        if ':wkb_aoi' in v:
            orcCur.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
            bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
            df = read_query(orcCnx, orcCur, v ,bvars)
        else:
            df = pd.read_sql(v, orcCnx)
        
        print (f'......{len(df)} rows')
        data_dict[k]= df
        
        counter+=1
    
    return data_dict


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

        counter += 1


            
if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'
    gdb= os.path.join(wks, 'inputs', 'data.gdb')
    
    print ('Connecting to databases')    
    
    print ('..connect to BCGW') 
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    orcCnx= Oracle.connection
    orcCur= Oracle.cursor
    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'to_flp_thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn 
    dckCnx.execute("SET GLOBAL pandas_analyze_sample=1000000")
    
    print ('\nReading the AOI file')
    gdf_aoi= esri_to_gdf(os.path.join(gdb, 'aoi'))
    wkb_aoi, srid= get_wkb_srid(gdf_aoi)
    
    

    try:
        print ('\nReading input data')
        orSql= load_Orc_sql ()
        
        loc_dict= {}
        loc_dict['vri']= os.path.join(gdb, 'vri')
        #loc_dict['aoi_test']= os.path.join(gdb, 'aoi')
        
        data_dict= {}
        print('..reading from Oracle')
        #data_dict= read_oracle_data (orcCnx, orcCur, orSql, wkb_aoi, srid, data_dict)
        
        print('\n..reading from Local files')
        data_dict= read_local_data(loc_dict, data_dict)
        
        print('\nWriting data to duckdb')
        add_data_to_duckdb(dckCnx, data_dict)
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
        Duckdb.disconnect_db()
    
        
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  

         
