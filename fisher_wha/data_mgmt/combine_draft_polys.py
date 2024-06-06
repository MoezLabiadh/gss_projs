
import os
import pandas as pd
import geopandas as gpd

inputs_path= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\work\polygon_developement\shapes'

gdfs = []

counter= 1
for file in os.listdir(inputs_path):
    if file.endswith('.shp'):
        print (f'adding polygon {counter}: {file}')
        file_path = os.path.join(inputs_path, file)
        gdf = gpd.read_file(file_path)
        
        polygon_id = file.replace('polygon_', '').replace('.shp', '')
        gdf['POLYGON_ID']= polygon_id
        
        gdfs.append(gdf)
        
        counter+= 1

# Combine all individual GeoDataFrames into one
gdf_all = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

gdf_all['POLYGON_HA']= round(gdf_all.geometry.area/10000, 2)

gdf_all['WHA_TAG']= ''
gdf_all['FEAT_NOTES']= ''
gdf_all['FCODE']= ''
gdf_all['HARVEST']= ''




gdf_all= gdf_all[['POLYGON_ID', 'POLYGON_HA','WHA_TAG',
                  'FEAT_NOTES','FCODE','HARVEST','geometry']]

wks= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs'
gdf_all.to_file(os.path.join(wks,'Example_Fisher_WHA_polygons_ALL_05JUNE2024.shp'))


'''
extent = gdf_all.geometry.unary_union.envelope
gdf_env = gpd.GeoDataFrame(geometry=[extent], crs= gdf_all.crs)

gdf_env.to_file(os.path.join(wks,'Draft_Fisher_Polygons_ENVELOPE.shp'))
'''
