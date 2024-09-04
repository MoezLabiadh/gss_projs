import warnings
warnings.simplefilter(action='ignore')

import os
import json
import timeit
import cx_Oracle
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
          
    orSql['tsa']="""
        SELECT
            TSA_NUMBER_DESCRIPTION,
            COMMENTS,
            
            ROUND(SDO_GEOM.SDO_AREA(
                tsa.GEOMETRY, 0.5, 'unit=SQ_KM'), 2) AS TSA_AREA_SQKM,
            
            ROUND(SDO_GEOM.SDO_AREA(
                SDO_GEOMETRY(:wkb_aoi, :srid), 0.5, 'unit=SQ_KM'), 2) AS QS_AREA_SQKM,
            
            ROUND(SDO_GEOM.SDO_AREA(
                SDO_GEOM.SDO_INTERSECTION(
                    tsa.GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 0.5), 0.5, 'unit=SQ_KM'), 2) OVERLAP_AREA_SQKM,
            
            ROUND((SDO_GEOM.SDO_AREA(
                SDO_GEOM.SDO_INTERSECTION(
                  tsa.GEOMETRY, SDO_GEOMETRY(:wkb_aoi, :srid), 0.5), 0.5,'unit=SQ_KM') /
                     SDO_GEOM.SDO_AREA(
                       tsa.GEOMETRY, 0.5, 'unit=SQ_KM')) * 100, 0) AS OVERLAP_PERCENTAGE
            
        
        FROM
            WHSE_ADMIN_BOUNDARIES.FADM_TSA tsa
        
        WHERE
            TSB_NUMBER IS NULL 
            AND RETIREMENT_DATE IS NULL
            AND SDO_RELATE (tsa.GEOMETRY, 
                              SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
        """     
        
    return orSql
    

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
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\ForestStewardship\LandscapePlanning\2024028_TO_TSA_maps'
    print ('Connecting to BCGW.')
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    connection= Oracle.connection
    cursor= Oracle.cursor
    
    print ('Read QS shapefile')
    f= os.path.join(wks, 'inputs', 'QS Territories in BC Without Lines.shp')
    gdf= esri_to_gdf (f)
    
    wkb_aoi, srid= get_wkb_srid(gdf)
    
    
    try:
        print('Running the query')
        orSql= load_Orc_sql()
        cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
        bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
        df = read_query(connection,cursor,orSql['tsa'],bvars)
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
        
    
    outloc= os.path.join(wks, 'outputs')
    generate_report (outloc, [df], ['query result'],'query_TSAs_QS_signatory_boundary')
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')   