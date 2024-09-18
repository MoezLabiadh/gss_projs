import warnings
warnings.simplefilter(action='ignore')

import os
import json
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
import timeit
from datetime import datetime, timedelta


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


def load_queries ():
    sql = {}

    sql ['idf_ctblk'] = """
        SELECT
            MGMT_UNIT_DESCRIPTION,
            OPENING_ID,
            FOREST_FILE_ID,
            CUTTING_PERMIT_ID,
            TIMBER_MARK,
            CUT_BLOCK_ID,
            MAP_LABEL,
            FILE_TYPE_CODE,
            OPENING_STATUS_CODE,
            PREV_TREE_SPECIES1_CODE,
            PREV_TREE_SPECIES2_CODE,
            PREV_AGE_CLASS_CODE,
            DISTURBANCE_START_DATE,
            DISTURBANCE_END_DATE,
            CLIENT_NAME,
            GENERALIZED_BGC_ZONE_CODE,
            GENERALIZED_BGC_SUBZONE_CODE,
            DENUDATION_1_DISTURBANCE_CODE,
            DENUDATION_1_SILV_SYSTEM_CODE,
            OPENING_GROSS_AREA
        
        FROM WHSE_FOREST_VEGETATION.RSLT_OPENING_SVW
        
        WHERE 
            REGION_CODE= 'RTO'
            AND MGMT_UNIT_DESCRIPTION IN ('100 Mile House TSA' , 'Kamloops TSA' , 
                                          'Merritt TSA' , 'Okanagan TSA')
            AND GENERALIZED_BGC_ZONE_CODE= 'IDF'
            AND DENUDATION_1_DISTURBANCE_CODE = 'L'
            AND SDO_RELATE (GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
                    """                   

    return sql


def create_report (df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""

    writer = pd.ExcelWriter(filename+'.xlsx',engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})
        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.close()


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'
    
    print ('Connecting to BCGW.')
    oracle_connector = OracleConnector()
    oracle_connector.connect_to_db()
    connection= oracle_connector.connection
    cursor= oracle_connector.cursor
    
    print ('\nRead AOI')
    gdb= os.path.join(wks, 'inputs', 'data.gdb')
    aoi= os.path.join(gdb, 'aoi')
    gdf= esri_to_gdf (aoi)
    wkb_aoi, srid= get_wkb_srid(gdf)
    
    print ('\nRun query')
    sql= load_queries ()
    cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
    bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
    df = read_query(connection,cursor,sql['idf_ctblk'],bvars)
    
    
    print ('\nCompute stats')
    past_fifteen_years = datetime.now() - timedelta(days=15*365)
    df_fy = df[df['DISTURBANCE_END_DATE'] >= past_fifteen_years]

    df_sum = df_fy.groupby('DENUDATION_1_SILV_SYSTEM_CODE')['OPENING_GROSS_AREA'].sum().reset_index()
    total_area = df_fy['OPENING_GROSS_AREA'].sum()
    
    df_sum.rename(columns={
            'DENUDATION_1_SILV_SYSTEM_CODE': 'DENUDATION_SILV_SYSTEM',
            'OPENING_GROSS_AREA': 'TOTAL_AREA_HA'
        }, inplace= True
    )
    
    df_sum['AREA_%'] = round(df_sum['TOTAL_AREA_HA'] / total_area * 100, 1)
    
    df_sum.sort_values(by='AREA_%', ascending=False, inplace=True)
    
    
    print ('\nExport results')
    df_list= [df, df_sum]
    sheet_list= ['query_result_All', '15Year_summary']
    
    datetime= datetime.now().strftime("%Y%m%d_%H%M")
    outfile= os.path.join(wks, 'outputs', f'{datetime}_partialCut_analysis')
    
    create_report (df_list, sheet_list,outfile)
    
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds') 
    