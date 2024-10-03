import duckdb


db= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240819_flp_to_thlb_analysis\inputs\tor_flp_thlb_analysis.db'
conn = duckdb.connect(db)

conn.install_extension('spatial')
conn.load_extension('spatial')

#conn.execute("""DROP TABLE IF EXISTS rip_kam_thlb;""")
#conn.execute("""ALTER TABLE thlb_plan_areas RENAME TO thlb_QS;""")
#conn.execute("""ALTER TABLE idf_thlb_tsa_mdwr DROP COLUMN IF EXISTS geometry;""")
#conn.execute("""ALTER TABLE idf_thlb_tsa_mdwr RENAME COLUMN geometry_1 TO geometry;""")

#conn.execute("""CREATE INDEX idx_rip_thlb ON rip_fbp_thlb_tsa USING RTREE (geometry);""")
#conn.execute("""CREATE INDEX idx_ogda ON ogda USING RTREE (geometry);""")
#conn.execute("""CREATE INDEX idx_idf ON idf USING RTREE (geometry);""")


tabs= conn.execute("""SHOW TABLES""").df()


sql= """SELECT* EXCLUDE GEOMETRY FROM ogda"""
#sql= """SELECT*  FROM idf_thlb"""



df= conn.execute(sql).df()


conn.close()