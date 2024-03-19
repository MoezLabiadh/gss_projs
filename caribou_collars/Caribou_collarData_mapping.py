import os
import pandas as pd
import arcpy
from arcgis.features import GeoAccessor
from datetime import datetime, timedelta

def prep_collar_data(in_lotek_nmc,in_lotek_cmc,in_vectr,in_attr):
    """Return a dataframe of lastfix info"""
    #read data
    print ('...processing lotek data')
    df_lotek_nmc= pd.read_csv(in_lotek_nmc)
    df_lotek_cmc= pd.read_csv(in_lotek_cmc)

    df_lotek= pd.concat([df_lotek_cmc, df_lotek_nmc], ignore_index=True)
    df_lotek.reset_index(drop=True, inplace=True)

    del_cols_lotek = ['Back [V]','Main [V]', 'Fix Status','Device Name',
                      'Date & Time [Local]']
    df_lotek.drop(columns=del_cols_lotek, inplace= True)

    print ('...processing vectronics data')
    df_vectr= pd.read_csv(in_vectr)
    
    del_cols_vectr = ['Beacon [V]', 'Main[V]', 'Sats Used', 'Fix Type', 
                      'ECEF Z[m]', 'ECEF Y[m]', 'ECEF X[m]']
    df_vectr.drop(columns=del_cols_vectr, inplace= True)

    #unify columns
    df_lotek['Provider']= 'Lotek'
    df_vectr['Provider']= 'Vectronic'

    df_lotek.rename(columns={
         'Device ID': 'Collar_ID',
         'Date & Time [GMT]': 'Date_Time_UTC',
         'Date & Time [Local]': 'Date_Time_Local',
         'Temp [C]': 'Temp_C'}, inplace=True)

    df_vectr.rename(columns={
         'Collar ID': 'Collar_ID',
         'Acq. Time [UTC]': 'Date_Time_UTC',
         'Latitude[deg]': 'Latitude',
         'Longitude[deg]': 'Longitude',
         'Altitude[m]': 'Altitude',
         'Temp[°C]': 'Temp_C'}, inplace=True)

    df_gps= pd.concat([df_lotek, df_vectr], ignore_index=True)
    df_gps.reset_index(drop=True, inplace=True)

    #keep only the last fix
    print ('...preparing a Last Fix dataset')
    df_gps['Date_Time_UTC'] = pd.to_datetime(df_gps['Date_Time_UTC'])
    latest_indices = df_gps.groupby('Collar_ID')['Date_Time_UTC'].idxmax()

    df_lstFix = df_gps.loc[latest_indices]

    #drop rows with missing lat and/or long
    df_lstFix.dropna(subset=['Longitude', 'Latitude'], inplace= True)
    df_lstFix.reset_index(drop=True, inplace=True)
    
    #check if coordinates are within valid range
    def coord_valid(lat, long):
        return (50 <= lat <= 60) and (-130 <= long <= -120)
    
    df_lstFix['Valid_Coordinates'] = df_lstFix.apply(
                lambda row: 'Yes' if coord_valid(
                    row['Latitude'], row['Longitude']) else 'No', axis=1)

    #add a new column to record last fixes older than 3 weeks
    three_weeks_ago = datetime.utcnow() - timedelta(weeks=3)
    df_lstFix['lstFix_Older_3weeks'] = df_lstFix['Date_Time_UTC']\
                               .apply(lambda x: 'Yes' if x < three_weeks_ago else 'No')

    #df_lstFix contains the last fix date and coordinates of deployed collars.
    #join these with the rest of attributes (herd name, range, comments...)
    df_attr= pd.read_csv(in_attr)
    df_attr.rename(columns=lambda x: x.replace('.', '_'), inplace= True)
    df = pd.merge(df_lstFix, df_attr, on='Collar_ID', how='left')

    return df


def prep_survey_data(in_survy):
    """Returns a dataframe containing survey information"""
    #ead data from csv
    df= pd.read_csv(in_survy)
    
    #unify date formats
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
    df['DATE'] = df['DATE'].fillna(pd.to_datetime(df['DATE'], format='%d-%b-%y', errors='coerce'))
    df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')
    
    #drop rows with missing lat and/or long
    df.dropna(subset=['Longitude', 'Latitude'], inplace= True)
    
    #check if coordinates are within valid range
    def coord_valid(lat, long):
        return (50 <= lat <= 60) and (-130 <= long <= -120)
    
    df['Valid_Coordinates'] = df.apply(
                lambda row: 'Yes' if coord_valid(
                    row['Latitude'], row['Longitude']) else 'No', axis=1)
    
    #keep only rows from the past 3 years/survey seasons
    current_year = datetime.now().year
    three_years_ago = current_year - 3
    df['DATE'] = pd.to_datetime(df['DATE'])
    df = df[df['DATE'].dt.year >= three_years_ago]
    
    #remove time from date col
    df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')
    
    return df


def df_to_fc (df, gdb_path, fc_name):
    """Creates a Featureclass based on a pandas df"""
    print (f'...exporting {fc_name}')
    df= df.loc[df['Valid_Coordinates']=='Yes']
    sedf = pd.DataFrame.spatial.from_xy(df= df,
                                        x_column='Longitude', 
                                        y_column='Latitude', 
                                        sr= 4326)
    arcpy.env.workspace = gdb_path
    arcpy.env.overwriteOutput = True

    sedf.spatial.to_featureclass(location=os.path.join(gdb_path, fc_name))


def export_layer_to_kmz(aprx_file, layers, out_kmz_path):
    """Exports layers to KMLs"""
    aprx = arcpy.mp.ArcGISProject(aprx_file)

    map_obj = aprx.listMaps("Main Map")[0]

    for lyr in map_obj.listLayers():
        if lyr.name in layers:
            print (f'...exporting KMZ: {lyr.name}')
            arcpy.conversion.LayerToKML(lyr, os.path.join(out_kmz_path,lyr.name))
            
            
def export_pdf_map(aprx_file, layout_name, out_pdf):
    """Exports a PDF map """   
    aprx = arcpy.mp.ArcGISProject(aprx_file)    
    layout = aprx.listLayouts(layout_name)[0]   
    layout.exportToPDF(out_pdf, 150, image_quality="BEST", georef_info=True)


if __name__ == "__main__":
    wks= r'\\spatialfiles.bcgov\Work\srm\kam\Workarea\ksc_proj\Wildlife\2020314_Caribou_collar_pointData_automation'
    indata= os.path.join(wks, 'data', 'inputs')
    
    #Input CSVss
    #lotek collars
    in_lotek_nmc= os.path.join(indata, 'GPS_011224_081624-NMC-Lotek.csv')
    in_lotek_cmc= os.path.join(indata, 'GPS_011224_081727-CMC-Lotek.csv')
    
    #vectronic collars
    in_vectr= os.path.join(indata, 'GPS-CMC-NMC-Vectronic.csv')
    
    #surveys
    in_survy= os.path.join(indata, 'Surveys-CMC-NMC.csv')
    
    #extra attributes (range, comments...)
    in_attr= os.path.join(indata, 'extra_attributes.csv')
    
    print ('Load Collar data')
    df_lstFix= prep_collar_data(in_lotek_nmc,in_lotek_cmc,in_vectr,in_attr)
    
    print ('\nLoad Survey data')
    df_survey= prep_survey_data(in_survy)
    
    print ('Create Feature classes')
    gdb_path= os.path.join(wks, 'data', 'collar_data.gdb')
    fc_name= 'active_collars_lastFix'
    df_to_fc (df_lstFix, gdb_path, fc_name)
    
    fc_name= 'lastFix_older_3weeks'
    df_lstFix_old= df_lstFix.loc[df_lstFix['lstFix_Older_3weeks']=='Yes']
    df_to_fc (df_lstFix_old, gdb_path, fc_name)
    
    fc_name= 'surveys'
    df_to_fc (df_survey, gdb_path, fc_name)
    
    print ('\nExport KMZs')
    aprx_file= os.path.join(wks, 'ArcGIS','Caribou_collarData_2.aprx')
    out_kmz_path= os.path.join(wks, 'outputs')

    layers= ['Active Collars - Last Fix', 
             'Last Fix Older than 3 weeks', 
             'Surveys - 3 past seasons']

    export_layer_to_kmz(aprx_file, layers, out_kmz_path)
    
    print('\nExport a PDF map')
    layout_name= 'EsizeP'
    out_pdf= os.path.join(wks, 'outputs', 'collars_map.pdf')
    export_pdf_map(aprx_file, layout_name, out_pdf)
    
    
    
    
    
    print ('\nProcessing Completed!')