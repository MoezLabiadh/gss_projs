
import os
import pandas as pd
import geopandas as gpd

wks= r'\\spatialfiles.bcgov\Work\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs'

#draft polys
shp_dr= os.path.join(wks,'Draft_Fisher_Polygons_ALL.shp')

gdf_dr= gpd.read_file(shp_dr)


mask = gdf_dr['POLYGON_ID'].str.contains('Polygon')
gdf_dr.loc[mask, 'POLYGON_ID'] = gdf_dr.loc[mask, 'POLYGON_ID']\
    .str.split('Polygon', expand=True)[1]\
        .str.replace('_', '')\
            .str.strip()
            
gdf_dr.to_file(shp_dr)


#revised polys
shp_rv= os.path.join(wks,'Revised_Fisher_Polygons_ALL.shp')

gdf_rv= gpd.read_file(shp_rv)

gdf_dr['REVISED'] = 'No'
gdf_rv['REVISED'] = 'Yes'

gdf_all= gpd.GeoDataFrame(pd.concat([gdf_dr, gdf_rv]).reset_index(drop=True))

gdf_all['POLYGON_HA']= round(gdf_all.geometry.area/10000, 2)

gdf_all= gdf_all[['POLYGON_ID', 'POLYGON_HA','REVISED' ,'geometry']]

gdf_all.to_file(os.path.join(wks,'Draft_Fisher_WHA_ALL.shp'))


extent = gdf_all.geometry.unary_union.envelope
gdf_env = gpd.GeoDataFrame(geometry=[extent], crs= gdf_all.crs)

gdf_env.to_file(os.path.join(wks,'Draft_Fisher_Polygons_ENVELOPE.shp'))

