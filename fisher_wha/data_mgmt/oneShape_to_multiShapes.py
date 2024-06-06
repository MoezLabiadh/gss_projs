import os
import geopandas as gpd

gdb= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs\data.gdb'
outloc= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\work\polygon_developement\shapes'

gdf= gpd.read_file(filename= gdb, layer='Draft_Fisher_WHA_ALL')

polys= gdf['POLYGON_ID'].to_list()

gdf= gdf[['POLYGON_ID', 'POLYGON_HA', 'geometry']]

c= 1
for poly in polys:
    print(f'working on poly {c} of {len(polys)}: {poly}')
    gdf_poly= gdf[gdf['POLYGON_ID']== poly]
    file_path= os.path.join(outloc, f'polygon_v2_{poly}.shp')
    
    gdf_poly.to_file(file_path)
    
    
    c+=1
    
    