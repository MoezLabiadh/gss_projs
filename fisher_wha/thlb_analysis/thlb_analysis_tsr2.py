import os
import timeit
import duckdb
import pandas as pd
from datetime import datetime

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
            

def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df



def load_dck_sql():
    dkSql= {}

    dkSql['poly_thlb_mature']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          thmt.INCLFACT,
          thmt.CONTCLAS,
          ROUND(ST_Area(ST_Intersection(thmt.geometry, poly.geometry))/10000,4)  AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(thmt.geometry, poly.geometry))* thmt.INCLFACT)/10000,4) AS THLB_MATURE_HA

        FROM 
          draft_fisher_polys poly  
              JOIN thlb_tsr2_mature thmt 
                  ON ST_Intersects(thmt.geometry, poly.geometry)
                    """

    dkSql['poly_uwr']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          uwg.UWR_NUMBER,
          uwg.TIMBER_HARVEST_CODE,
          thmt.INCLFACT,
          thmt.CONTCLAS, 
          ROUND(ST_Area(ST_Intersection(ST_Intersection(poly.geometry, uwg.geometry), thmt.geometry))/10000, 4) AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(ST_Intersection(poly.geometry, uwg.geometry), thmt.geometry))) * thmt.INCLFACT/10000, 4) AS UWR_THLB_MATURE_HA
          
        FROM 
            draft_fisher_polys AS poly
            JOIN 
                uwg ON ST_Intersects(poly.geometry, uwg.geometry)
            JOIN 
                thlb_tsr2_mature AS thmt ON ST_Intersects(poly.geometry, thmt.geometry) 
                    AND ST_Intersects(uwg.geometry, thmt.geometry)
                    
        WHERE
            uwg.UWR_NUMBER IN ('u-7-022', 'u-7-020', 'u-7-013', 'u-7-011');
                    """

    dkSql['poly_vqo']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          vqo.REC_EVQO_CODE,
          vqo.SCENIC_AREA_IND,
          thmt.INCLFACT,
          thmt.CONTCLAS, 
          ROUND(ST_Area(ST_Intersection(ST_Intersection(poly.geometry, vqo.geometry), thmt.geometry))/10000, 4) AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(ST_Intersection(poly.geometry, vqo.geometry), thmt.geometry))) * thmt.INCLFACT/10000, 4) AS VQO_THLB_MATURE_HA
          
        FROM 
            draft_fisher_polys AS poly
            JOIN 
                vqo ON ST_Intersects(poly.geometry, vqo.geometry)
            JOIN 
                thlb_tsr2_mature AS thmt ON ST_Intersects(poly.geometry, thmt.geometry) 
                    AND ST_Intersects(vqo.geometry, thmt.geometry);
                    """

    dkSql['poly_cofa']="""
        SELECT 
          poly.DISTRICT,
          poly.POLYGON_ID,
          poly.REVISED,
          poly.POLYGON_HA,
          thmt.VRI_POLY_ID,
          thmt.PROJ_AGE_1 AS STAND_AGE,
          cofa.NON_LEGAL_OGMA_PROVID,
          thmt.INCLFACT,
          thmt.CONTCLAS, 
          ROUND(ST_Area(ST_Intersection(ST_Intersection(poly.geometry, cofa.geometry), thmt.geometry))/10000, 4) AS INTERSECT_HA,
          ROUND((ST_Area(ST_Intersection(ST_Intersection(poly.geometry, cofa.geometry), thmt.geometry))) * thmt.INCLFACT/10000, 4) AS COFA_THLB_MATURE_HA
          
        FROM 
            draft_fisher_polys AS poly
            JOIN 
                cofa ON ST_Intersects(poly.geometry, cofa.geometry)
            JOIN 
                thlb_tsr2_mature AS thmt ON ST_Intersects(poly.geometry, thmt.geometry) 
                    AND ST_Intersects(cofa.geometry, thmt.geometry);
                    """                    
    return dkSql

def run_duckdb_queries (dckCnx, dict_sqls):
    """Run duckdb queries """
    results= {}
    counter = 1
    for k, v in dict_sqls.items():
        print(f'..running query {counter} of {len(dict_sqls)}: {k}')
        results[k]= dckCnx.execute(v).df()
        
        counter+= 1
        
    return results


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
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
    gdb= os.path.join(wks, 'inputs', 'data.gdb')
    
    print ('Connect to databases')    
    
    print ('..connect to Duckdb') 
    projDB= os.path.join(wks, 'inputs', 'thlb_analysis.db')
    Duckdb= DuckDBConnector(db= projDB)
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn
    

    try:
        print ('Run Queries')
        dksql= load_dck_sql()
        q_rslt= run_duckdb_queries (dckCnx, dksql) 
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Duckdb.disconnect_db()

    print('Compute stats')

    df= q_rslt['poly_thlb_mature']
    grpCols= ['DISTRICT', 'POLYGON_ID', 'REVISED', 'POLYGON_HA']
    
    df= df.groupby(grpCols)['THLB_MATURE_HA'].sum().reset_index()
    
    df.sort_values(by=grpCols, inplace= True)
    
    
    #COFA (netdown 100%)
    cofa_factor= 1
    
    df_cofa= q_rslt['poly_cofa']
    df_cofa= df_cofa.groupby('POLYGON_ID')['COFA_THLB_MATURE_HA'].sum().reset_index()
    
    
    df_cofa['COFA_NETDOWN_FACTOR']= cofa_factor
    df_cofa['COFA_NETDOWN_THLB_HA']= df_cofa['COFA_THLB_MATURE_HA']* df_cofa['COFA_NETDOWN_FACTOR']
    
    df_cofa.sort_values(by='POLYGON_ID', inplace= True)
    
    
    #VQO netdowns
    vqo_factors= {'P': 1,
                  'R': 0.992,
                  'PR': 0.957,
                  'M': 0.874}

    df_vqo= q_rslt['poly_vqo']
    df_vqo= df_vqo.groupby(['POLYGON_ID', 'REC_EVQO_CODE'])['VQO_THLB_MATURE_HA'].sum().reset_index()
    
    
    df_vqo['VQO_NETDOWN_FACTOR']= df_vqo['REC_EVQO_CODE'].map(vqo_factors)
    df_vqo['VQO_NETDOWN_THLB_HA']= df_vqo['VQO_THLB_MATURE_HA']* df_vqo['VQO_NETDOWN_FACTOR']
    
    df_vqo.sort_values(by='POLYGON_ID', inplace= True)
    
    df_vqo_all= df_vqo.groupby('POLYGON_ID')['VQO_NETDOWN_THLB_HA'].sum().reset_index()
    

    #UWR netdowns
    uwr_no_factor = None #already accounted for in THLB
    uwr_cndt_factors= {'u-7-022': 0.5,
                       'u-7-020': 0.5,
                       'u-7-013': 0.4,
                       'u-7-011': 0.4,}

    
    df_uwr= q_rslt['poly_uwr']
    grpCols= ['POLYGON_ID', 'UWR_NUMBER', 'TIMBER_HARVEST_CODE']
    df_uwr= df_uwr.groupby(grpCols)['UWR_THLB_MATURE_HA'].sum().reset_index()
    
    df_uwr['UWR_NETDOWN_FACTOR']= df_uwr['UWR_NUMBER'].map(uwr_cndt_factors)
    df_uwr.loc[df_uwr['TIMBER_HARVEST_CODE'] == 'NO HARVEST ZONE', 'UWR_NETDOWN_FACTOR'] = uwr_no_factor
    
    df_uwr['UWR_NETDOWN_THLB_HA']= df_uwr['UWR_THLB_MATURE_HA']* df_uwr['UWR_NETDOWN_FACTOR']
    
    df_uwr.sort_values(by='POLYGON_ID', inplace= True)
    
    df_uwr_all= df_uwr.groupby('POLYGON_ID')['UWR_NETDOWN_THLB_HA'].sum().reset_index()
    
    
    #final  summary table
    df_sum = pd.merge(df, df_uwr_all, on='POLYGON_ID', how='left')
    df_sum = pd.merge(df_sum, df_vqo_all, on='POLYGON_ID', how='left')
    df_sum = pd.merge(df_sum, df_cofa[['POLYGON_ID', 'COFA_NETDOWN_THLB_HA']], on='POLYGON_ID', how='left')
    
    df_sum.fillna(0, inplace= True)
    
    df_sum['TOTAL_NETDOWN_THLB_HA']= df_sum['UWR_NETDOWN_THLB_HA'] + df_sum['VQO_NETDOWN_THLB_HA'] + df_sum['COFA_NETDOWN_THLB_HA']
    
    df_sum['IMPACT_THLB_HA']= df_sum['THLB_MATURE_HA'] - df_sum['TOTAL_NETDOWN_THLB_HA']
    
    
    print('Export final report')
    outloc= os.path.join(wks, 'outputs')
    today = datetime.today().strftime('%Y%m%d')
    filename= today + '_Fisher_draftPolys_thlbAnalysis_TSR2'
    df_list= [df_sum, df_uwr, df_vqo, df_cofa]
    sheet_list= ['SUMMARY', 'NETDOWN uwr', 'NETDOWN vqo', 'NETDOWN cofa']
    
    generate_report (outloc, df_list, sheet_list,filename)
        
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  