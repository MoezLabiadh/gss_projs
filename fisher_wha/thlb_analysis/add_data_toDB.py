import os
import json
import timeit
import cx_Oracle
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkb


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

 
def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def load_Orc_sql():
    orSql= {}

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
            SDO_WITHIN_DISTANCE (GEOMETRY, 
                                 SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=500 unit=m') = 'TRUE'
                    """
 
    orSql['uwg'] = """
        SELECT
            UWR_NUMBER,
            UWR_UNIT_NUMBER,
            SPECIES_1,
            TIMBER_HARVEST_CODE,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY 
        FROM
            WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP
        WHERE
            SDO_WITHIN_DISTANCE (GEOMETRY, 
                                 SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=500 unit=m') = 'TRUE'
                    """

    orSql['vqo'] = """
        SELECT
            VLI_POLYGON_NO,
            REC_EVQO_CODE,
            SCENIC_AREA_IND,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY 
        FROM
            WHSE_FOREST_VEGETATION.REC_VIMS_EVQO_SVW
        WHERE
            SDO_WITHIN_DISTANCE (GEOMETRY, 
                                 SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=500 unit=m') = 'TRUE'
                    """

    orSql['cofa'] = """
        SELECT
            NON_LEGAL_OGMA_PROVID,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY 
        FROM
            WHSE_LAND_USE_PLANNING.RMP_OGMA_NON_LEGAL_CURRENT_SVW
        WHERE
            SDO_WITHIN_DISTANCE (GEOMETRY, 
                                 SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=500 unit=m') = 'TRUE'
                    """
                    
    return orSql


def oracle_2_duckdb(orcCnx, orcCur, dckCnx, dict_sqls, wkb_aoi, srid):
    """Insert data from Oracle into a duckdb table"""
    tables = {}
    counter = 1
    
    for k, v in dict_sqls.items():
        print(f'..adding table {counter} of {len(dict_sqls)}: {k}')
        print('....export from Oracle')
        if ':wkb_aoi' in v:
            orcCur.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
            bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
            df = read_query(orcCnx, orcCur, v ,bvars)
        else:
            df = pd.read_sql(v, orcCnx)
            

        dck_tab_list= dckCnx.execute('SHOW TABLES').df()['name'].to_list()
        
        if k in dck_tab_list:
            dck_row_count= dckCnx.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0]
            dck_col_nams= dckCnx.execute(f"""SELECT column_name 
                                             FROM INFORMATION_SCHEMA.COLUMNS 
                                             WHERE table_name = '{k}'""").df()['column_name'].to_list()
                
            if (dck_row_count != len(df)) or (set(list(df.columns)) != set(dck_col_nams)):
                print (f'....import to Duckdb ({df.shape[0]} rows)')
                create_table_query = f"""
                CREATE OR REPLACE TABLE {k} AS
                  SELECT * EXCLUDE geometry, ST_GeomFromText(geometry) AS GEOMETRY
                  FROM df;
                """
                dckCnx.execute(create_table_query)
            else:
                print('....data already in db: skip importing')
                pass
        
        else:
            print (f'....import to Duckdb ({df.shape[0]} rows)')
            create_table_query = f"""
            CREATE OR REPLACE TABLE {k} AS
              SELECT * EXCLUDE geometry, ST_GeomFromText(geometry) AS GEOMETRY
              FROM df;
            """
            dckCnx.execute(create_table_query)
      
        df = df.drop(columns=['GEOMETRY'])
        
        tables[k] = df
      
        counter += 1

    return tables


def gdf_to_duckdb (dckCnx, loc_dict):
    """Insert data from a gdfs into a duckdb table """
    tables = {}
    counter= 1
    for k, v in loc_dict.items():
        print (f'..adding table {counter} of {len(loc_dict)}: {k}')
        print ('....export from gdb')
        df= esri_to_gdf (v)
        df['GEOMETRY']= df['geometry'].apply(lambda x: wkb.dumps(x, output_dimension=2))
        df = df.drop(columns=['geometry'])
        
        dck_tab_list= dckCnx.execute('SHOW TABLES').df()['name'].to_list()
        
        if k in dck_tab_list:
            dck_row_count= dckCnx.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0]
            dck_col_nams= dckCnx.execute(f"""SELECT column_name 
                                             FROM INFORMATION_SCHEMA.COLUMNS 
                                             WHERE table_name = '{k}'""").df()['column_name'].to_list()
                
            if (dck_row_count != len(df)) or (set(list(df.columns)) != set(dck_col_nams)):
                print (f'....import to Duckdb ({df.shape[0]} rows)')
                create_table_query = f"""
                CREATE OR REPLACE TABLE {k} AS
                  SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS GEOMETRY
                  FROM df;
                """
                dckCnx.execute(create_table_query)
            else:
                print('....data already in db: skip importing')
                pass
        
        else:
            print (f'....import to Duckdb ({df.shape[0]} rows)')
            create_table_query = f"""
            CREATE OR REPLACE TABLE {k} AS
              SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS GEOMETRY
              FROM df;
            """
            dckCnx.execute(create_table_query)
            
        
        df = df.drop(columns=['GEOMETRY'])
        
        tables[k] = df
        
        counter+= 1
        
    return tables    


def load_dck_sql():
    dkSql= {}
    dkSql['thlb_tsr2_mature']="""
        --Drop table if exists
        DROP TABLE IF EXISTS thlb_tsr2_mature;
        
        --Create table
        CREATE TABLE thlb_tsr2_mature AS
            SELECT 
              vri.POLYGON_ID AS VRI_POLY_ID,
              vri.PROJ_AGE_1,
              thlb.INCLFACT,
              thlb.CONTCLAS,
              ROUND(ST_Area(ST_Intersection(vri.geometry, thlb.geometry))/10000,2)  AS THLB_MATURE_HA,
              ST_Intersection(vri.geometry, thlb.geometry) AS geometry
            FROM 
              vri vri JOIN thlb_tsr2 thlb ON ST_Intersects(vri.geometry, thlb.geometry)
            WHERE 
              vri.PROJ_AGE_1 >= 80;
          
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
    
    print ('..connect to BCGW') 
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    orcCnx= Oracle.connection
    orcCur= Oracle.cursor
    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn
    
    print ('Read AOI dataset')   
    aoi= os.path.join(gdb, 'Draft_Fisher_WHA_ALL_AOI')
    gdf_aoi= esri_to_gdf (aoi)
    wkb_aoi, srid= get_wkb_srid(gdf_aoi)
    
    
    try:

        print ('\nLoad BCGW datasets')
        orSql= load_Orc_sql ()
        #orcTables= oracle_2_duckdb(orcCnx, orcCur, dckCnx, orSql, wkb_aoi, srid)
        
        print ('\nLoad local datasets')
        
        loc_dict={}
        loc_dict['draft_fisher_polys']= os.path.join(gdb, 'Draft_Fisher_WHA_ALL')
        loc_dict['thlb_tsr2']= os.path.join(gdb, 'thlb_tsr2')
        #gdbTables= gdf_to_duckdb (dckCnx, loc_dict)

        print ('Run Queries')
        dksql= load_dck_sql()
        rslts= run_duckdb_queries (dckCnx, dksql) 
        
        df= dckCnx.execute("SELECT* FROM thlb_tsr2_mature").df()
        df = df.drop(columns=['geometry'])

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