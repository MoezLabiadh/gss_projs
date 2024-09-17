import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import duckdb
import numpy as np
import pandas as pd

class DuckDBConnector:
    def __init__(self, db=':memory:'):
        self.db = db
        self.conn = None
    
    def connect_to_db(self):
        """Connects to a DuckDB database and installs spatial extension."""
        self.conn = duckdb.connect(self.db)
        self.conn.install_extension('spatial')
        self.conn.load_extension('spatial')
        return self.conn
    
    def disconnect_db(self):
        """Disconnects from the DuckDB database."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            


    

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis'

    print ('Connecting to databases')    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'tor_flp_thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn 
    dckCnx.execute("SET GLOBAL pandas_analyze_sample=1000000")
    
    try:

        print ('\nCompute Gross THLB summaries')
        # thlb by TSA (whole tsa)
        df_tlhb_tsa= dckCnx.execute("""SELECT* EXCLUDE geometry FROM tsa_full_thlb""").df()
        df_tlhb_tsa_sum = df_tlhb_tsa.groupby(['TSA_NAME'])[['THLB_AREA']].sum().reset_index().rename(columns={'THLB_AREA': 'TSA_THLB_AREA'})
        
        df_tlhb_tsa_sum['RIP_ADJUST_FACTOR'] = np.where(
                df_tlhb_tsa_sum['TSA_NAME'] == '100 Mile House TSA', 0.02,
                np.where(df_tlhb_tsa_sum['TSA_NAME'] == 'Okanagan TSA', 0.13, 0)
        )
        
        df_tlhb_tsa_sum['TSA_THLB_AREA_RIP_ADJUSTED'] = df_tlhb_tsa_sum['TSA_THLB_AREA'] - (
            df_tlhb_tsa_sum['TSA_THLB_AREA'] * df_tlhb_tsa_sum['RIP_ADJUST_FACTOR']
        )
       
        # thlb by TSA (in plan area)
        df_tlhb_tsaPlan= dckCnx.execute("""SELECT*  EXCLUDE geometry FROM tsa_planA_thlb""").df() 
        df_tlhb_tsaPlan_sum = df_tlhb_tsaPlan.groupby(['TSA_NAME'])[['THLB_AREA']].sum().reset_index().rename(columns={'THLB_AREA': 'AOI_THLB_AREA'})
       

        print ('Compute IDF summaries')
        #100 mile house scenarios
        df_idf= dckCnx.execute("""SELECT* EXCLUDE geometry FROM idf_thlb_tsa_mdwr""").df()
        
        df_idf_omh= df_idf[df_idf['TSA_NAME']=='100 Mile House TSA']
        df_idf_omh['IDF_REDUCTION_FACTOR']= 0.5
        
        bec_excl= ['mm', 'mw', 'dk', 'xh', 'xm', 'dw', 'xw', 'ww']
        #df_idf_omh = df_idf_omh[~df_idf_omh['BEC_SUBZONE'].isin(bec_excl)]
        

        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        Duckdb.disconnect_db()
        
        
finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
        