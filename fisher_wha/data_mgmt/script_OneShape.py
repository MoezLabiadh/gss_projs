
import os
import pandas as pd
import geopandas as gpd

polylist= ['Draft_Fisher_Polygon243', 'Draft_Fisher_Polygon242', 'Draft_Fisher_Polygon241', 'Draft_Fisher_Polygon240', 'Draft_Fisher_Polygon239', 
    'Draft_Fisher_Polygon238', 'Draft_Fisher_Polygon237', 'Draft_Fisher_Polygon236', 'Draft_Fisher_Polygon235c', 'Draft_Fisher_Polygon234', 
    'Draft_Fisher_Polygon233', 'Draft_Fisher_Polygon232e', 'Draft_Fisher_Polygon231c', 'Draft_Fisher_Polygon230c', 'Draft_Fisher_Polygon229', 
    'Draft_Fisher_Polygon228e', 'Draft_Fisher_Polygon227g', 'Draft_Fisher_Polygon226', 'Draft_Fisher_Polygon225', 'Draft_Fisher_Polygon224c', 
    'Draft_Fisher_Polygon223', 'Draft_Fisher_Polygon327', 'Draft_Fisher_Polygon326c', 'Draft_Fisher_Polygon325e', 'Draft_Fisher_Polygon324', 
    'Draft_Fisher_Polygon323', 'Draft_Fisher_Polygon322', 'Draft_Fisher_Polygon222', 'Draft_Fisher_Polygon221', 'Draft_Fisher_Polygon220c', 
    'Draft_Fisher_Polygon321c', 'Draft_Fisher_Polygon219g', 'Draft_Fisher_Polygon320c', 'Draft_Fisher_Polygon319', 'Draft_Fisher_Polygon318g', 
    'Draft_Fisher_Polygon317', 'Draft_Fisher_Polygon_316b', 'Draft_Fisher_Polygon315', 'Draft_Fisher_Polygon314', 'Draft_Fisher_Polygon311', 
    'Draft_Fisher_Polygon310', 'Draft_Fisher_Polygon309', 'Draft_Fisher_Polygon308c', 'Draft_Fisher_Polygon307c', 'Draft_Fisher_Polygon306b', 
    'Draft_Fisher_Polygon305', 'Draft_Fisher_Polygon304c', 'Draft_Fisher_Polygon218c', 'Draft_FisherPolygon217', 'Draft_FisherPolygon216', 
    'Draft_Fisher_Polygon215', 'Draft_Fisher_Polygon214e', 'Draft_Fisher_Polygon213c', 'Draft_Fisher_Polygon212e', 'Draft_Fisher_Polygon211', 
    'Draft_Fisher_Polygon210', 'Draft_Fisher_Polygon209c', 'Draft_Fisher_Polygon208', 'Draft_Fisher_Polygon207', 'Draft_Fisher_Polygon206', 
    'Draft_Fisher_Polygon205', 'Draft_Fisher_Polygon204e', 'Draft_Fisher_Polygon203c', 'Draft_Fisher_Polygon303', 'Draft_Fisher_Polygon302', 
    'Draft_Fisher_Polygon301', 'Draft_Fisher_Polygon300d', 'Draft_Fisher_Polygon202c', 'Draft_Fisher_Polygon201', 'Draft_Fisher_Polygon200', 
    'Draft_Fisher_Polygon199', 'Draft_Fisher_Polygon198', 'Draft_Fisher_Polygon12e', 'Draft_Fisher_Polygon195', 'Draft_Fisher_Polygon197',
    'Draft_Fisher_Polygon194', 'Draft_Fisher_Polygon193', 'Draft_Fisher_Polygon192', 'Draft_Fisher_Polygon191b', 'Draft_Fisher_Polygon190', 
    'Draft_Fisher_Polygon188f', 'Draft_Fisher_Polygon189', 'Draft_Fisher_Polygon187g', 'Draft_Fisher_Polygon186', 'Draft_Polygon185f', 
    'Draft_Fisher_Polygon184c', 'Draft_Fisher_Polygon183c', '182c', 'Draft_Fisher_Polygon180b', 'Draft_Fisher_Polygon181d', 
    'Draft_Fisher_Polygon179e']


in_folder= r'W:\wlap\prg\Workarea\lmckinley\Projects\WHA_FRPA\Fisher_PG_TSA\TEST'
wks= 'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
gdf_list=[]
for filename in os.listdir(in_folder):
    file_path = os.path.join(in_folder, filename)
    for n in polylist:
        if filename.endswith(".shp"):
            if n == filename[:-4]:
                gdf= gpd.read_file(file_path)
                gdf['POLYGON_ID']= filename[:-4]
                gdf= gdf[['POLYGON_ID', 'geometry']]
                gdf_list.append(gdf)
                

gdf_all= gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))

gdf_all= gdf_all.dissolve(by='POLYGON_ID')


gdf_all.reset_index(inplace=True)

gdf_all['POLYGON_HA']=  round(gdf_all['geometry'].area/ 10000,2)

out_shp= os.path.join(wks, 'inputs', 'Draft_Fisher_Polygons_ALL.shp')
gdf_all.to_file(out_shp)