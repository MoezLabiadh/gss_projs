import warnings
warnings.simplefilter(action='ignore')

import os
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


def multipart_to_singlepart(gdf):
    """Converts a multipart gdf to singlepart gdf """
    gdf['dissolvefield'] = 1
    gdf = gdf.dissolve(by='dissolvefield')
    gdf.reset_index(inplace=True)
    gdf = gdf[['geometry']] #remove all columns
         
    return gdf


def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]
    wkb_aoi = wkb.dumps(geom)
    
    # if geometry has Z values, flatten geometry
    if geom.has_z:
        wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def load_queries ():
    sql = {}

    sql ['geomCol'] = """
                    SELECT column_name GEOM_NAME
                    
                    FROM  ALL_SDO_GEOM_METADATA
                    
                    WHERE owner = :owner
                        AND table_name = :tab_name
                        
                    """                   
                                         
    sql ['intersect_wkb'] = """
                    SELECT {cols}, 
                        ROUND(SDO_GEOM.SDO_AREA(
                            SDO_GEOM.SDO_INTERSECTION(
                                SDO_CS.TRANSFORM({geom_col}, 1000003005, 3005),
                              SDO_GEOMETRY(:wkb_aoi, :srid), 0.005), 0.005, 'unit=HECTARE'), 5) OVERLAP_AREA_HA
                    
                    FROM {tab}
                    
                    WHERE SDO_RELATE ({geom_col}, 
                                      SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
                        {def_query}  
                        """

    return sql


def get_geom_colname (connection,cursor,table,geomQuery):
    """ Returns the geometry column of BCGW table name: can be either SHAPE or GEOMETRY"""
    el_list = table.split('.')

    bvars_geom = {'owner':el_list[0].strip(),
                  'tab_name':el_list[1].strip()}
    df_g = read_query(connection,cursor,geomQuery, bvars_geom)
    
    geom_col = df_g['GEOM_NAME'].iloc[0]

    return geom_col


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

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    
    
if __name__ == "__main__":
    """ Runs statusing"""
    workspace = r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
    print ('Connecting to BCGW.')
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    connection= Oracle.connection
    cursor= Oracle.cursor
    
    print ('Reading tool inputs.')
    rule_xls = os.path.join(workspace,'scripts','statusing','statusing_rules.xlsx')
    df_stat = pd.read_excel(rule_xls, 'rules')
    df_stat.fillna(value='nan',inplace=True)
    
    gdb= os.path.join(workspace,'inputs','data.gdb')
    fc= 'Draft_Fisher_WHA_ALL_05JUN2024'
    gdf_polys= gpd.read_file(gdb, layer=fc)
    polys= gdf_polys['POLYGON_ID'].tolist()
    #polys=['003c', '004e', '146', '147', '323', '324',] #test polys
    
    try:
        print ('Running Analysis.')
        sql = load_queries ()
        
        results = {} 
        c_names = 1
        for index, row in df_stat.iterrows(): 
            name = row['Name']
            table = row['Dataset']
            cols = row['Columns']
            print (f"\n...overlapping {c_names} of {df_stat.shape[0]}: {name}")
        
            if row['Where'] != 'nan':
                def_query = 'AND ' + row['Where']
            else:
                def_query = ' '
            
            c_names += 1
             
            c = 1
            dfs = []
            for poly in polys:
                print (f".....working on Polygon {c} of {str(len(polys))}: {poly}")
                gdf_p = gdf_polys.loc[gdf_polys['POLYGON_ID'] == poly]
                
                if gdf_p.shape[0] > 1:
                    gdf_p =  multipart_to_singlepart(gdf_p) 
                    
                wkb_aoi,srid = get_wkb_srid (gdf_p)
                
                if table.startswith('WHSE'):
                    geomQuery = sql ['geomCol']
                    geom_col = get_geom_colname (connection,cursor,table,geomQuery)
                    
                    query = sql ['intersect_wkb'].format(cols=cols,tab=table,
                                                         def_query=def_query, geom_col=geom_col)
                    cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
                    bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
                    df = read_query(connection,cursor,query,bvars)
               
                else:
                    gdf_trg = esri_to_gdf (table)
                    if not gdf_trg.crs.to_epsg() == 3005:
                            gdf_trg = gdf_trg.to_crs({'init': 'epsg:3005'})
                            
                    gdf_intr = gpd.overlay(gdf_p, gdf_trg, how='intersection')
                    gdf_intr['OVERLAP_AREA_HA'] = gdf_intr['geometry'].area/ 10**6
                    df = pd.DataFrame(gdf_intr)
                    cols_d = []
                    cols_d.append(cols)
                    cols_d.append('OVERLAP_AREA_HA')
                    df = df[cols_d]
        
                df ['POLYGON_ID'] = poly
                dfs.append (df)
                
                c += 1
            
            df_res = pd.concat(dfs).reset_index(drop=True) 
            cols_res = [col for col in df_res.columns if col != 'POLYGON_ID']
            cols_res.insert(0,'POLYGON_ID')
            df_res = df_res[cols_res]
            
            df_res = df_res.loc[df_res['OVERLAP_AREA_HA'] > 0]
            df_res = df_res.sort_values('POLYGON_ID')
            
            if df_res.shape [0] < 1:
                df_res = df_res.append({'POLYGON_ID' : 'NO OVERLAPS FOUND!'}, ignore_index=True)
            
            results[name] =  df_res  

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
        
    finally: 
        Oracle.disconnect_db()    
    
    print ('\nGenerating the status Report.')
    outloc= os.path.join(workspace, 'outputs') 
    today = datetime.today().strftime('%Y%m%d')
    filename = today + '_fisherPolys_statusing'
    df_list = list(results.values())
    sheet_list = list(results.keys())
    generate_report (outloc, df_list, sheet_list,filename)

