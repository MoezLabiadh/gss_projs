import os
import timeit
import pandas as pd
import arcpy

def add_buffer_widths(fc, gdb, buffer_df):
    fc_path = os.path.join(gdb, fc)
    
    # Add new fields
    arcpy.AddField_management(fc_path, "buffer_width_fbp", "DOUBLE")
    arcpy.AddField_management(fc_path, "buffer_width_kam", "DOUBLE")
    
    # Create a dictionary for faster lookup
    buffer_dict = buffer_df.set_index('Riparian Class').to_dict()
    
    # Update values based on matching riparian class
    with arcpy.da.UpdateCursor(fc_path, ["riparian_class", "buffer_width_fbp", "buffer_width_kam"]) as cursor:
        for row in cursor:
            riparian_class = row[0]
            if riparian_class in buffer_dict['Buffer width FBP']:
                row[1] = buffer_dict['Buffer width FBP'][riparian_class]
                row[2] = buffer_dict['Buffer width KAM'][riparian_class]
                cursor.updateRow(row)

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    # Load the pandas dataframe with buffer width information
    wks = r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'
    gdb = os.path.join(wks, 'inputs', 'data.gdb')
    specs_file = os.path.join(wks, 'inputs', 'riparian_buffers.xlsx')
    buffer_df = pd.read_excel(specs_file)
    
    # List of feature classes to update
    feature_classes = ['rivers_3tsas', 'lakes_3tsas', 'wetlands_3tsas','streams_3tsas']
    
    # Set the workspace
    arcpy.env.workspace = gdb
    
    for fc in feature_classes:
        print(f"\n..processing {fc}")
        add_buffer_widths(fc, gdb, buffer_df)
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int(t_sec/60)
    secs = int(t_sec%60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')