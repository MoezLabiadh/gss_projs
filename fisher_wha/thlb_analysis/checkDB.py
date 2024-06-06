import duckdb

db= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs\thlb_analysis.db'
conn = duckdb.connect(db)

conn.install_extension('spatial')
conn.load_extension('spatial')

tabs= df= conn.execute("""SHOW TABLES""").df()


sql= """SELECT* EXCLUDE GEOMETRY FROM vqo"""


df= conn.execute(sql).df()
    
