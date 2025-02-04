import duckdb


db= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis\inputs\tor_flp_thlb_analysis.db'
conn = duckdb.connect(db)

conn.install_extension('spatial')
conn.load_extension('spatial')


#conn.execute("""DROP TABLE IF EXISTS r3_idf_vri_thlb;""")
#conn.execute("""ALTER TABLE thlb_plan_areas RENAME TO thlb_QS;""")

#conn.execute("""ALTER TABLE idf_thlb_tsa_mdwr DROP COLUMN IF EXISTS geometry;""")
#conn.execute("""ALTER TABLE idf_thlb_tsa_mdwr RENAME COLUMN geometry_1 TO geometry;""")

#conn.execute("""DROP INDEX idx_r3_idf_vri_thlb;""")
#conn.execute("""CREATE INDEX idx_r2_2_rip_thlb_mdwr_fullattr ON r2_2_rip_thlb_mdwr_fullattr USING RTREE (geometry);""")

#conn.execute("""PRAGMA max_temp_directory_size='50GiB';""")

tabs= conn.execute("""SHOW TABLES""").df()



sql= """SELECT* EXCLUDE geometry FROM r3_rip_vri_thlb"""
#sql= """SELECT*  FROM r2_2_rip_idf_ogda_thlb_mdwr"""


df= conn.execute(sql).df()

df['FBP_THLB_HA'] = df['AREA_HA'] * df['thlb_fact']   

df_gr = df.groupby(
                ['TSA_NAME']
                )[['FBP_THLB_HA']].sum().reset_index()



conn.close()