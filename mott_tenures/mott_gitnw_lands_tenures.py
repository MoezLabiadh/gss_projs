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
    shp = os.path.join("Map A shapefiles","Gitanyow House Territories adjusted to rivers BCalbers.shp")
    gdf = esri_to_gdf (shp)

    sql = """
    SELECT * FROM(
    SELECT
        CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
        CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
        DS.FILE_CHR AS FILE_NBR,
        SG.STAGE_NME AS STAGE,
        --TT.ACTIVATION_CDE,
        TT.STATUS_NME AS STATUS,
        --DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
        TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
        TY.TYPE_NME AS TENURE_TYPE,
        ST.SUBTYPE_NME AS TENURE_SUBTYPE,
        PU.PURPOSE_NME AS TENURE_PURPOSE,
        SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
        --DT.DOCUMENT_CHR,
        --DT.RECEIVED_DAT AS RECEIVED_DATE,
        --DT.ENTERED_DAT AS ENTERED_DATE,
        DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
        DT.EXPIRY_DAT AS EXPIRY_DATE,
        --IP.AREA_CALC_CDE,
        IP.AREA_HA_NUM AS AREA_HA,
        DT.LOCATION_DSC,
        OU.UNIT_NAME,
        --IP.LEGAL_DSC,
        CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
        SH.SHAPE
        
    FROM WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT 
    JOIN WHSE_TANTALIS.TA_INTEREST_PARCELS IP 
        ON DT.DISPOSITION_TRANSACTION_SID = IP.DISPOSITION_TRANSACTION_SID
        AND IP.EXPIRY_DAT IS NULL
    JOIN WHSE_TANTALIS.TA_DISP_TRANS_STATUSES TS
        ON DT.DISPOSITION_TRANSACTION_SID = TS.DISPOSITION_TRANSACTION_SID 
        AND TS.EXPIRY_DAT IS NULL
    JOIN WHSE_TANTALIS.TA_DISPOSITIONS DS
        ON DS.DISPOSITION_SID = DT.DISPOSITION_SID
    JOIN WHSE_TANTALIS.TA_STAGES SG 
        ON SG.CODE_CHR = TS.CODE_CHR_STAGE
    JOIN WHSE_TANTALIS.TA_STATUS TT 
        ON TT.CODE_CHR = TS.CODE_CHR_STATUS
    JOIN WHSE_TANTALIS.TA_AVAILABLE_TYPES TY 
        ON TY.TYPE_SID = DT.TYPE_SID    
    JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBTYPES ST 
        ON ST.SUBTYPE_SID = DT.SUBTYPE_SID 
        AND ST.TYPE_SID = DT.TYPE_SID 
    JOIN WHSE_TANTALIS.TA_AVAILABLE_PURPOSES PU 
        ON PU.PURPOSE_SID = DT.PURPOSE_SID    
    JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBPURPOSES SP 
        ON SP.SUBPURPOSE_SID = DT.SUBPURPOSE_SID 
        AND SP.PURPOSE_SID = DT.PURPOSE_SID 
    JOIN WHSE_TANTALIS.TA_ORGANIZATION_UNITS OU 
        ON OU.ORG_UNIT_SID = DT.ORG_UNIT_SID 
    JOIN WHSE_TANTALIS.TA_TENANTS TE 
        ON TE.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
        AND TE.SEPARATION_DAT IS NULL
        AND TE.PRIMARY_CONTACT_YRN = 'Y'
    JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
        ON PR.INTERESTED_PARTY_SID = TE.INTERESTED_PARTY_SID
        
    LEFT JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
        ON SH.INTRID_SID = IP.INTRID_SID) TN
        
    WHERE 
        TN.STATUS = 'DISPOSITION IN GOOD STANDING' 
        AND TN.CLIENT_NAME_PRIMARY LIKE '%MINISTRY OF TRANSPORTATION%'
        AND SDO_RELATE (TN.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'

    
    ORDER BY TN.EFFECTIVE_DATE DESC
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
        
        # Optional: Export to Excel
        workspace = os.path.dirname(shp)
        output_filename = 'Tenure_Intersections_by_House_Territory'
        generate_report(workspace, [final_df], ['Intersections'], output_filename)
        print(f'\nReport saved to: {os.path.join(workspace, output_filename)}.xlsx')
    else:
        print('\n\nNo intersecting records found for any polygons.')
        final_df = pd.DataFrame()

        
    # Close database connection
    cursor.close()
    connection.close()
    print('\nDatabase connection closed.')
