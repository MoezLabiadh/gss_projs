import duckdb

db= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons\inputs\thlb_analysis.db'
conn = duckdb.connect(db)

tabs= df= conn.execute("""SHOW TABLES""").df()
df= conn.execute("""SELECT* EXCLUDE GEOMETRY FROM thlb_curr_mature""").df()