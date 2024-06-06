'''
Reclassify a slope percent raster to defined slope classses
'''
import os
import rasterio
from rasterio.features import shapes
import geopandas as gpd
import numpy as np
from shapely.geometry import shape
import timeit

start_t = timeit.default_timer() #start time 

# Define file path
wks= r'\\spatialfiles.bcgov\Work\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs'
slope_raster = os.path.join(wks, 'Slope', 'slope_pct_AOI.tif')

# Open the input slope raster
with rasterio.open(slope_raster) as src:
    # Read the raster data
    print('Reading the slope raster')
    slope_data = src.read(1)
    
    #Mask noData values
    slope_data = np.ma.masked_array(slope_data, mask=(slope_data == src.nodata))


    # Define the reclassification thresholds
    print('Reclassifying the slope raster')
    reclassify_bins = [10, 20, 30, 40, 50, 60, 70, np.max(slope_data)]
    reclassify_labels = ['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70+', 'NoData']
    
    # Reclassify the slope values
    reclassified_data = np.digitize(slope_data, reclassify_bins, right=True)
    
    # Convert reclassified_data to a supported dtype
    reclassified_data = reclassified_data.astype(np.uint8)
    
    # Get the crs of the input geotiff
    source_crs = src.crs
    
    # Write the reclassified data to a new GeoTIFF file
    output_tif= slope_raster = os.path.join(wks,'Slope', 'py_reclass_test.tif')
    with rasterio.open(
        output_tif,
        'w',
        driver='GTiff',
        height=reclassified_data.shape[0],
        width=reclassified_data.shape[1],
        count=1,
        dtype=reclassified_data.dtype,
        crs=source_crs,
        transform=src.transform,
    ) as dst:
        dst.write(reclassified_data, 1)
        
    # Generate shapes and values from the reclassified data
    print('Vectorizing the reclassified raster')
    shapes_and_values = shapes(reclassified_data, transform=src.transform)
    
    # Create a list of geometries and values
    geometries = []
    values = []
    for geom, value in shapes_and_values:
        geometries.append(shape(geom))  # Convert to shapely geometry
        values.append(value)
    
    # Create a GeoDataFrame
    reclassified_gdf = gpd.GeoDataFrame({'slope_class_id': values, 'geometry': geometries})
    
    #reclassified_gdf.crs= source_crs
    
    # Cast slope_class_id to integers
    reclassified_gdf['slope_class_id'] = reclassified_gdf['slope_class_id'].astype(int)
    
    # Map reclassify_labels to the values
    reclassified_gdf['slope_class_label'] = reclassified_gdf['slope_class_id'].apply(lambda x: reclassify_labels[x])
    
    # Remove NoData values
    reclassified_gdf= reclassified_gdf.loc[reclassified_gdf['slope_class_label']!='NoData']
    
    # Dissolve the GeoDataFrame by 'slope_class_label'
    #dissolved_gdf = reclassified_gdf.dissolve(by='slope_class_label')



# Export the GeoDataFrame to a geodatabse
print('Exporting the slope class vector to project gdb')
out_gdb= os.path.join(wks,'data.gdb')
reclassified_gdf.to_file(out_gdb, layer= 'slope_class', driver="OpenFileGDB") 


finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  

