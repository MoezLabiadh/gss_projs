import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor = connection.cursor()
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection, cursor


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


def get_wkb_srid(geom, srid):
    """Returns WKB object from geometry"""
    wkb_aoi = wkb.dumps(geom)
    
    # if geometry has Z values, flatten geometry
    if geom.has_z:
        wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi



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

    #writer.save()
    writer.close()



if __name__ == "__main__":

    workspace = r"W:\srm\gss\sandbox\mlabiadh\workspace\20251127_2025_1426"

    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)

    print ('Reading the shapefile.')
    shp = os.path.join("Map A shapefiles","Map A shapefiles","Gitanyow House Territories adjusted to rivers BCalbers.shp")
    gdf = esri_to_gdf (shp)

    sql = """
    SELECT * 
    FROM
         WHSE_WATER_MANAGEMENT.WLS_WATER_APPROVALS_GOV_SVW wtr
    WHERE 
        wtr.CLIENT_NAME LIKE '%Ministry of Transportation%'
        AND SDO_RELATE (wtr.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
    """

    # Initialize list to store results
    all_results = []
    
    # Get SRID once (same for all polygons)
    srid = gdf.crs.to_epsg()
    
    print(f'\nProcessing {len(gdf)} polygons from shapefile...')
    
    # Iterate through each polygon in the GDF
    for idx in range(len(gdf)):
        # Get House and Territory values
        house = gdf.loc[idx, 'House']
        territory = gdf.loc[idx, 'Territory']
        
        print(f'\nProcessing polygon {idx+1}/{len(gdf)}: House={house}, Territory={territory}')
        
        # Get geometry and convert to WKB
        geom = gdf['geometry'].iloc[idx]
        wkb_aoi = get_wkb_srid(geom, srid)
        
        # Set input sizes and execute query
        cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
        bvars = {'wkb_aoi': wkb_aoi, 'srid': srid}
        
        # Execute query for this polygon
        df_result = read_query(connection, cursor, sql, bvars)
        
        # Add House and Territory columns to the result
        if not df_result.empty:
            df_result['House'] = house
            df_result['Territory'] = territory
            all_results.append(df_result)
            print(f'  Found {len(df_result)} intersecting records')
        else:
            print(f'  No intersecting records found')
    
    # Combine all results into a single dataframe
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)

        # Remove SHAPE column if it exists
        if 'SHAPE' in final_df.columns:
            final_df = final_df.drop(columns=['SHAPE'])
        
        # Reorder columns to put House and Territory at the beginning
        cols = final_df.columns.tolist()
        cols.remove('House')
        cols.remove('Territory')
        final_df = final_df[['House', 'Territory'] + cols]

        print(f'\n\nTotal records found: {len(final_df)}')
        print('\nFinal DataFrame columns:', list(final_df.columns))
        print('\nFirst few rows:')
        print(final_df.head())
        
        # Optional: Export to Excel
        output_filename = 'MOTT_water_approvals_by_Gitanyow_House_Territory'
        generate_report(workspace, [final_df], ['Intersections'], output_filename)
        print(f'\nReport saved to: {os.path.join(workspace, output_filename)}.xlsx')
    else:
        print('\n\nNo intersecting records found for any polygons.')
        final_df = pd.DataFrame()

        
    # Close database connection
    cursor.close()
    connection.close()
    print('\nDatabase connection closed.')
