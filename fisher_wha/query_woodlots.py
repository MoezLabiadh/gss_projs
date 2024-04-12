import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import json
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
from datetime import datetime


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
            

def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df

            
def create_aoi(in_shp):
    """Return the extent of the draft polys """
    gdf_all= gpd.read_file(in_shp)
    extent = gdf_all.geometry.unary_union.envelope
    gdf = gpd.GeoDataFrame(geometry=[extent], crs= gdf_all.crs)
    
    return gdf


def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid

def load_queries ():
    sql = {}                                              
    sql['wdlts'] = """
        SELECT
            FOREST_FILE_ID,
            MAP_BLOCK_ID,
            ML_TYPE_CODE,
            ROUND(SDO_GEOM.SDO_AREA(GEOMETRY, 0.5, 'unit=HECTARE'), 2) AS AREA_HA,
            LIFE_CYCLE_STATUS_CODE,
            CLIENT_NUMBER,
            CLIENT_NAME,
            ADMIN_DISTRICT_CODE
            
        FROM 
            WHSE_FOREST_TENURE.FTEN_MANAGED_LICENCE_POLY_SVW   
            
        WHERE 
            FEATURE_CLASS_SKEY in ( 865, 866) 
            AND LIFE_CYCLE_STATUS_CODE <> 'RETIRED'
            AND SDO_WITHIN_DISTANCE (GEOMETRY, 
                                     SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=100 unit=m') = 'TRUE'
                        """

    return sql


def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    outfile= os.path.join(workspace, filename + '.xlsx')

    writer = pd.ExcelWriter(outfile,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        #workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 25)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
    
    print ('Connecting to BCGW.')
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    connection= Oracle.connection
    cursor= Oracle.cursor
    
    print ('Create an AOI shape.')
    in_shp= os.path.join(wks, 'inputs', 'Draft_Fisher_Polygons_ALL.shp')
    gdf= create_aoi(in_shp)
    #gdf.to_file(os.path.join(wks, 'tests', 'union.shp'))
    wkb_aoi, srid= get_wkb_srid(gdf)
    
    try:
        print ('\nRun the Woodlot query.')
        sql= load_queries ()
        cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
        bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
        df = read_query(connection,cursor,sql['wdlts'],bvars)
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
    
    
    print ('\nExport the report.')
    ouloc= os.path.join(wks, 'outputs')
    today = datetime.today().strftime('%Y%m%d')
    filename= today + '_Fisher_polys_AOI_woodlots'
    generate_report (ouloc, [df], ['woodlots'], filename)
    
        
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
    