import pandas as pd
import geopandas as gpd
import os

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


xls=r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\tests\NEW_DPG_DVA_polygons_for_digitizing.xlsx'
fc= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs\data.gdb\Draft_Fisher_WHA_ALL'

df= pd.read_excel(xls, 'NEW_polygons_DPG')
gdf= esri_to_gdf (fc)

gdf = gdf.loc[(gdf['DISTRICT'] == 'DPG') & (gdf['REVISED'] == 'No')]

gdf_l= [str(x).strip() for x in gdf['POLYGON_ID'].to_list()]
df_l= [str(x).strip() for x in df['NEW Polygons'].to_list()]

difference = [x for x in df_l if x not in gdf_l]