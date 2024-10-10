import duckdb


db= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis\inputs\tor_flp_thlb_analysis.db'
conn = duckdb.connect(db)

conn.install_extension('spatial')
conn.load_extension('spatial')

#conn.execute("""DROP TABLE IF EXISTS r2_rip_overlap_ogda_thlb;""")
#conn.execute("""ALTER TABLE thlb_plan_areas RENAME TO thlb_QS;""")

#conn.execute("""ALTER TABLE idf_thlb_tsa_mdwr DROP COLUMN IF EXISTS geometry;""")
#conn.execute("""ALTER TABLE idf_thlb_tsa_mdwr RENAME COLUMN geometry_1 TO geometry;""")

#conn.execute("""CREATE INDEX idx_r2_2_rip_ogda ON r2_2_rip_ogda USING RTREE (geometry);""")

#conn.execute("""PRAGMA max_temp_directory_size='50GiB';""")

tabs= conn.execute("""SHOW TABLES""").df()

#sql= """SELECT* EXCLUDE geometry FROM r2_2_rip_idf_thlb_mdwr"""
sql= """SELECT*  FROM r2_2_rip_idf_thlb_mdwr"""



df= conn.execute(sql).df()


conn.close()