from select import select
import pandas as pd
import geopandas as gpd
import pyodbc

# variables
from credenciales_bbdd import base_de_datos_config

tabla_name = "ciclovias_geo"
# importar datos

data = gpd.read_file(
    r"C:\Users\ismael\Desktop\html\coder data\referencia_geografica_ciclovias_WGS84.geojson"
)

gdf = gpd.GeoDataFrame(data, geometry="geometry")
print(gdf.head())
# conectar a base de datos

conn = pyodbc.connect(**base_de_datos_config)
cursor = conn.cursor()


# crear tabla

try:
    cursor.execute(f"select * from {tabla_name}")
except pyodbc.ProgrammingError:
    cursor.execute(
        f"""
        CREATE TABLE {tabla_name} (
            id int IDENTITY(1,1) PRIMARY KEY,
            codigo varchar(50),
            nombre_oficial varchar(255),
            longitud float,
            barrio varchar(255),
            geometry geometry
        )
        """
    )
    conn.commit()

# insertar datos

for index, row in gdf.iterrows():
    cursor.execute(
        f"""
        INSERT INTO {tabla_name} (codigo, nombre_oficial, longitud, barrio, geometry)
        VALUES (?, ?, ?, ?, geometry::STGeomFromText(?, 0))
        """,
        row["codigo"],
        row["nomoficial"],
        row["long"],
        row["BARRIO"],
        row["geometry"].wkt,
    )
    conn.commit()
