import sqlalchemy as sa

api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImJhb2FuaDdhQGdtYWlsLmNvbSIsInNlc3Npb24iOiJiYW9hbmg3YS5nbWFpbC5jb20iLCJwYXQiOiJBYUMzdUVZdXdEc3E3b0RuQVpqRmpsSlVOTERiOWxPZnpDcDRQUXJ4RDdNIiwidXNlcklkIjoiZmRiMmJkZDAtMTdjYS00ZGIzLTg5MzItMTY2ZWEwZGJkYzcwIiwiaXNzIjoibWRfcGF0IiwicmVhZE9ubHkiOmZhbHNlLCJ0b2tlblR5cGUiOiJyZWFkX3dyaXRlIiwiaWF0IjoxNzU3NDM3ODUzfQ.MyN0hvVw5azfQng-2pnmP4qsjmghq1E-FKERky8lPRM"

# SQLAlchemy URI cho MotherDuck
uri = f"duckdb:///md:jobs?motherduck_token={api_key}"

engine = sa.create_engine(uri)

with engine.connect() as conn:
    result = conn.execute(sa.text("SHOW TABLES")).fetchall()
    print("Tables in MotherDuck database:")
    for row in result:
        print(row)
