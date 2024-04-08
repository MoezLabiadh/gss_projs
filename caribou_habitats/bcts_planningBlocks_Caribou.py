import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkb
from datetime import datetime


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
            

def process_bcts_blocks (in_folder):
    """""Returns a gdf of bcts blocks"""
    blks= []
    for filename in os.listdir(in_folder):
         if filename.endswith(".shp"):
             print(f'..processing {filename}')
             filepath = os.path.join(in_folder, filename)
             gdf = gpd.read_file(filepath)
             
             gdf['Area_ha']= round(gdf['geometry'].area /10000,2)
             
             gdf= gdf[['Block_ID', 'Area_ha', 'geometry']]
             gdf.dropna(subset=['Block_ID'], inplace= True)
             
             blks.append(gdf)
             
    gdf= pd.concat(blks, ignore_index=True)
    gdf.reset_index(drop=True, inplace= True)
    
    return gdf

            
def process_habitat_polys (rng, in_folder):
    """""Returns a gdf of habitat polygons"""
    polys= []
    for filename in os.listdir(in_folder):
         if filename.endswith(".shp"):
             print(f'..processing {filename}')
             lyr = os.path.splitext(filename)[0]
             filepath = os.path.join(in_folder, filename)
             gdf = gpd.read_file(filepath)
             
             gdf['Range']= rng
             gdf['Source']= lyr
             
             #flatten geometries 3D to 2D
             _drop_z = lambda geom: wkb.loads(wkb.dumps(geom, output_dimension=2))
             gdf.geometry = gdf.geometry.transform(_drop_z)
             
             gdf= gdf[['Range','Source','geometry']]
             polys.append(gdf)
             
    gdf= pd.concat(polys, ignore_index=True)
    gdf.reset_index(drop=True, inplace= True)
    
    return gdf


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
    
    wks= r'\\spatialfiles.bcgov\Work\srm\kam\Workarea\ksc_proj\Wildlife\20240326_BCTS_planningBlocks_Caribou_habitats' 

    print ('Connecting to duckdb')
    Duckdb= DuckDBConnector()
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn

    print ('\nReading BCTS blocks')
    bk_folder= os.path.join(wks, 'inputs', 'bcts_blocks')
    gdf_blks= process_bcts_blocks (bk_folder)
    
    print ('\nReading Chase habitat files')
    cs_folder= os.path.join(wks, 'inputs', 'chase_polys')
    gdf_cs= process_habitat_polys ('Chase', cs_folder)
    
    print ('\nReading Wolverine habitat files')
    wv_folder= os.path.join(wks, 'inputs', 'wolverine_polys')
    gdf_wv= process_habitat_polys ('Wolverine', wv_folder)

    print ('\nReading Biologist files')
    bi_folder= os.path.join(wks, 'inputs', 'biologists_polys')
    gdf_bi= process_habitat_polys ('Chase', bi_folder)
    
    print ('\nAdding data to duckdb')
    gdf_polys= pd.concat([gdf_cs, gdf_wv, gdf_bi], ignore_index=True)
    gdf_polys.reset_index(drop=True, inplace= True)
    
    #################### without Matrix Layers #############################
    #gdf_polys = gdf_polys.loc[~gdf_polys['Source'].str.contains('Matrix')]
    #################### without Matrix Layers #############################
    
    #################### without Calving Layers #############################
    #gdf_polys = gdf_polys.loc[~gdf_polys['Source'].str.contains('calving')]
    #################### without Calving Layers #############################
    
    
    gdf_to_duckdb (dckCnx, gdf_blks, 'bcts_blocks')
    gdf_to_duckdb (dckCnx, gdf_polys, 'habitat_polys')

    try:
        query="""
            SELECT
                blk.Block_ID,
                hbt.Range,
                hbt.Source,
                blk.Area_ha AS Block_Area_ha,
                ROUND(ST_Area(
                    ST_Intersection(
                        hbt.geometry, blk.geometry)::geometry) / 10000.0, 2) AS Overlap_Area_ha,
                ROUND(((ST_Area(
                   ST_Intersection(
                       blk.geometry, hbt.geometry)) / 10000.0) / blk.Area_ha) * 100,1) AS Overlap_pct
            FROM 
                bcts_blocks blk 
            JOIN 
                habitat_polys hbt 
            ON 
                ST_Intersects(hbt.geometry, blk.geometry);
            """
            
        query_buffer="""
            SELECT
                blk.Block_ID,
                hbt.Range,
                hbt.Source,
                blk.Block_BufArea_ha,
                ROUND(ST_Area(
                    ST_Intersection(
                        blk.geom_buf, hbt.geometry)::geometry) / 10000.0, 2) AS Overlap_Area_ha,
                ROUND(((ST_Area(
                    ST_Intersection(
                        blk.geom_buf, hbt.geometry)) / 10000.0) / blk.Block_BufArea_ha) * 100, 1) AS Overlap_pct
            FROM 
                (SELECT 
                    Block_ID,
                    ROUND(ST_Area(ST_Buffer(geometry, 500)) / 10000.0, 2) AS Block_BufArea_ha,
                    ST_Buffer(geometry, 500) AS geom_buf
                 FROM 
                    bcts_blocks) AS blk
            JOIN 
                habitat_polys hbt 
            ON 
                ST_Intersects(hbt.geometry,blk.geom_buf);
                """    
        
        print('\nExecuting queries')
        df_blk= dckCnx.execute(query).df()
        df_buf= dckCnx.execute(query_buffer).df()
        
        print('\nCalulcating stats')
        gdf_blks= gdf_blks[['Block_ID', 'geometry']]
        df_blks_all= gdf_blks[['Block_ID']]
        
        dfs = []
        dfs_stats = []
        gdfs= []
        for df in [df_blk, df_buf]:
            df = df.loc[df['Overlap_pct'] > 0.1]
            dfs.append(df)
            
            df_stat = df.groupby('Block_ID').size().reset_index(name='Nbr_overlaps')
            df_stat = pd.merge(df_blks_all, df_stat, how='left', on='Block_ID')
            df_stat['Nbr_overlaps'].fillna(0, inplace=True)
            dfs_stats.append(df_stat)
            
            gdf_stat= pd.merge(df_stat, gdf_blks, how='left', on='Block_ID')
            gdfs.append(gdf_stat)
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Duckdb.disconnect_db()
        
    print('\nExporting results')    
    out_loc= os.path.join(wks, 'outputs')
    today = datetime.today().strftime('%Y%m%d')
    
    #shapes
    shp_name_blk= today + '_bcts_planningBlocks_caribouHabitat_stats.shp'
    shp_name_buf= today + '_bcts_planningBlocks_caribouHabitat_stats_BUFFERS.shp'
    
    gpd.GeoDataFrame(gdfs[0]).to_file(os.path.join(out_loc, shp_name_blk))
    gpd.GeoDataFrame(gdfs[1]).to_file(os.path.join(out_loc, shp_name_buf))

    #report
    filename = today + '_bcts_planningBlocks_caribouHabitat_stats'
    df_list= dfs + dfs_stats
    sheet_list = ['overlap list - no buffer',
                  'overlap list - buffer',
                  'overlap stats - no buffer',
                  'overlap stats - buffer']
    
    generate_report (out_loc, df_list, sheet_list,filename)
    
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')   