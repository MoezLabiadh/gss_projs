import warnings
warnings.simplefilter(action='ignore')

import os
import json
import timeit
import cx_Oracle
import pandas as pd

class OracleConnector:
    def __init__(self, dbname='BCGW'):
        self.dbname = dbname
        self.cnxinfo = self.get_db_cnxinfo()

    def get_db_cnxinfo(self):
        """ Retrieves db connection params from the config file"""
        with open(r'H:\config\db_config.json', 'r') as file:
            data = json.load(file)
        
        if self.dbname in data:
            return data[self.dbname]
        
        raise KeyError(f"Database '{self.dbname}' not found.")
    
    def connect_to_db(self):
        """ Connects to Oracle DB and create a cursor"""
        try:
            self.connection = cx_Oracle.connect(self.cnxinfo['username'], 
                                                self.cnxinfo['password'], 
                                                self.cnxinfo['hostname'], 
                                                encoding="UTF-8")
            self.cursor = self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception(f'..Connection failed: {e}')

    def disconnect_db(self):
        """Close the Oracle connection and cursor"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("....Disconnected from the database")


def load_Orc_sql():
    orSql= {}

    orSql['ofpd'] = """
        SELECT 
            old.CURRENT_PRIORITY_DEFERRAL_ID AS ID,
            old.ANCIENT_FOREST_IND,
            old.REMNANT_OLD_ECOSYS_IND,
            CASE
                WHEN old.ANCIENT_FOREST_IND = 'Y' AND old.REMNANT_OLD_ECOSYS_IND = 'Y' THEN 'ANCIENT & REMNANT'
                WHEN old.ANCIENT_FOREST_IND = 'Y' THEN 'ANCIENT FOREST'
                WHEN old.REMNANT_OLD_ECOSYS_IND = 'Y' THEN 'REMNANT OLD ECOSYS'
                ELSE 'OLD FOREST'
            END AS CATEGORY,
            old.BGC_LABEL,
            REGEXP_SUBSTR(old.BGC_LABEL, '^[A-Z]+') AS BEC_ZONE,
            ROUND(SDO_GEOM.SDO_AREA(
                SDO_GEOM.SDO_INTERSECTION(
                    old.SHAPE, wsh.GEOMETRY, 0.005), 0.005, 'unit=HECTARE'), 2) AREA_HA
        FROM WHSE_FOREST_VEGETATION.OGSR_PRIORITY_DEF_AREA_CUR_SP old
        JOIN WHSE_BASEMAPPING.FWA_WATERSHED_GROUPS_POLY wsh
            ON SDO_RELATE(old.SHAPE, wsh.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
            AND wsh.WATERSHED_GROUP_CODE = 'THOM'
                    """
 
    orSql['ogma'] = """
        SELECT
            ogm_wshd.LEGAL_OGMA_PROVID AS ID,
            'OGMA' AS CATEGORY,
            bec.BGC_LABEL,
            bec.ZONE AS BEC_ZONE,
            ROUND(
                SDO_GEOM.SDO_AREA(
                        SDO_GEOM.SDO_INTERSECTION(bec.GEOMETRY, ogm_wshd.GEOMETRY, 0.05),
                    0.05, 'unit=HECTARE'
                ), 2
            ) AS AREA_HA
            
        FROM
            (SELECT
                ogm.LEGAL_OGMA_PROVID,
                SDO_GEOM.SDO_INTERSECTION(ogm.GEOMETRY, wsh.GEOMETRY, 0.05) AS GEOMETRY
             FROM  
                WHSE_LAND_USE_PLANNING.RMP_OGMA_LEGAL_CURRENT_SVW ogm
                INNER JOIN WHSE_BASEMAPPING.FWA_WATERSHED_GROUPS_POLY wsh
                    ON SDO_RELATE(ogm.GEOMETRY, wsh.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                     AND wsh.WATERSHED_GROUP_CODE = 'THOM'
            )ogm_wshd
            
            INNER JOIN WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY bec
                ON SDO_RELATE(bec.GEOMETRY, ogm_wshd.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                    """
                    
    return orSql

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

        worksheet.set_column(0, dataframe.shape[1], 25)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    
    

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\ForestStewardship\LandscapePlanning\2024028_TO_TSA_maps'

    print ('Connect to BCGW') 
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    orcCnx= Oracle.connection

    try:
        print('Execute queries')
        orSql= load_Orc_sql()
        print('...old forest def areas')
        df_ofpd= pd.read_sql(orSql['ofpd'] , orcCnx)
        
        print('...ogma')
        df_ogma= pd.read_sql(orSql['ogma'] , orcCnx)
        
        df= pd.concat([df_ofpd, df_ogma])
        
        df_sum = df.groupby(['CATEGORY', 'BEC_ZONE'])['AREA_HA'].sum().reset_index()

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
    
    
    print('Export report')
    outloc= os.path.join(wks, 'outputs', 'may2024')
    dfs= [df, df_sum]
    sheets= ['data', 'summary']
    filename= 'thompsonWSHD_oldForest_stats'
    
    generate_report (outloc, dfs, sheets,filename)
    
        
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  