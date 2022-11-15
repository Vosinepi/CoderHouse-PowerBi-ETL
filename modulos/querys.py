import pandas as pd
import geopandas as gpd

import pyodbc

pd.set_option("display.max_columns", None)

from variables import *

# variables
from credenciales_bbdd import base_de_datos_config


tabla_name_1 = "ciclovias_georeferenciadas"
tabla_name_2 = "puntos_censales_anuales"
tabla_name_3 = "puntos_censales_mensuales"
tabla_name_4 = "volumen_ciclista_anual"
tabla_name_5 = "volumen_ciclista_mensual"
# importar datos

ciclovias = gpd.read_file(ciclovias_geo)
puntos_censos_anuales = gpd.read_file(puntos_censos_anuales)
puntos_censos_mensuales = gpd.read_file(puntos_censos_mensuales)
volumen_ciclista_anual = pd.read_csv(volumen_ciclista_anual, dtype={"año": str})
volumen_ciclista_mensual = pd.read_csv(volumen_ciclista_mensual, dtype={"año": str})


gdf = ciclovias

# conectar a base de datos

conn = pyodbc.connect(**base_de_datos_config)
cursor = conn.cursor()


# crear tablas

#### ciclovias_georeferenciadas
def cargar_datos_ciclovias():
    print(gdf["id"].head(10))
    try:
        cursor.execute(f"select * from {tabla_name_1}")
    except pyodbc.ProgrammingError:
        cursor.execute(
            f"""
            CREATE TABLE {tabla_name_1} (
                id int IDENTITY(1,1) PRIMARY KEY,
                codigo varchar(50) NOT NULL,
                nombre_oficial varchar(255),
                alt_izquini int,
                alt_izqfin int,
                alt_derini int,
                alt_derfin int,
                nomanter varchar(255),
                nom_mapa varchar(255),
                tipo_c varchar(255),
                longitud float,
                sentido varchar(255),
                cod_sent int,
                observaciones varchar(255),
                bicisenda varchar(255), 
                lado_ciclo varchar(255),
                recorrid_x varchar(255),
                anio_construccion DATE,
                tooltip_bi varchar(255),
                red_jerarq varchar(255),
                red_tp varchar(255),
                comuna int,
                com_par int,
                com_impar int,
                barrio varchar(255),
                barrio_par varchar(255),
                barrio_impar varchar(255),
                geometry geometry,
            )
            """
        )
        conn.commit()

        # insertar datos
    contador = 0
    for index, row in gdf.iterrows():

        data = cursor.execute(f"SELECT id FROM {tabla_name_1} where id = {index+1}")

        data2 = data.fetchone()

        if data2 == None:

            cursor.execute(
                f"""
                INSERT INTO {tabla_name_1} (codigo, nombre_oficial, alt_izquini, alt_izqfin, alt_derini,alt_derfin,
                nomanter, nom_mapa, tipo_c, longitud, sentido, cod_sent, observaciones,
                bicisenda, lado_ciclo, recorrid_x, anio_construccion, tooltip_bi, red_jerarq, red_tp,
                comuna, com_par, com_impar, barrio, barrio_par, barrio_impar, geometry)
                VALUES (?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, geometry::STGeomFromText(?, 0))
                """,
                row["codigo"],
                row["nomoficial"],
                row["alt_izqini"],
                row["alt_izqfin"],
                row["alt_derini"],
                row["alt_derfin"],
                row["nomanter"],
                row["nom_mapa"],
                row["tipo_c"],
                row["long"],
                row["sentido"],
                row["cod_sent"],
                row["observa"],
                row["bicisenda"],
                row["lado_ciclo"],
                row["recorrid_x"],
                row["anio_construccion"],
                row["tooltip_bi"],
                row["red_jerarq"],
                row["red_tp"],
                row["COMUNA"],
                row["COM_PAR"],
                row["COM_IMPAR"],
                row["BARRIO"],
                row["BARRIO_PAR"],
                row["BARRIO_IMP"],
                row["geometry"].wkt,
            )
            conn.commit()
            contador += 1
            print(f"Insertando fila {contador} de {len(gdf)}")
        else:
            print(f"fila {index+1} de {len(gdf)} ya existe")

    cursor.close()
    conn.close()


def cargar_puntos_censos_anuales():
    print(puntos_censos_anuales["CENTROIDE"].head(10))
    try:
        cursor.execute(f"select * from {tabla_name_2}")
    except pyodbc.ProgrammingError:
        cursor.execute(
            f"""
            CREATE TABLE {tabla_name_2} (
                id int IDENTITY(1,1),
                x float,
                y float,
                recorrido varchar(255),
                centroide int PRIMARY KEY,
                cruce varchar(255),
                area varchar(255),
                geometry geometry,
            )
            """
        )
        conn.commit()

        # insertar datos
    contador = 0
    for index, row in puntos_censos_anuales.iterrows():

        data = cursor.execute(f"SELECT id FROM {tabla_name_2} where id = {index+1}")

        data2 = data.fetchone()

        if data2 == None:

            cursor.execute(
                f"""
                INSERT INTO {tabla_name_2} (x, y, recorrido, centroide, cruce, area, geometry)
                VALUES (?,?,?,?, ?, ?, geometry::STGeomFromText(?, 0))
                """,
                row["X"],
                row["Y"],
                row["RECORRIDO"],
                row["CENTROIDE"],
                row["CRUCE"],
                row["area"],
                row["geometry"].wkt,
            )
            conn.commit()
            contador += 1
            print(f"Insertando fila {contador} de {len(puntos_censos_anuales)}")
        else:
            print(f"fila {index+1} de {len(puntos_censos_anuales)} ya existe")

    cursor.close()
    conn.close()


def cargar_puntos_censos_mensuales():
    print(puntos_censos_mensuales["PB"].head(10))
    try:
        cursor.execute(f"select * from {tabla_name_3}")
    except pyodbc.ProgrammingError:
        cursor.execute(
            f"""
            CREATE TABLE {tabla_name_3} (
                id int IDENTITY(1,1),
                x float,
                y float,
                status varchar(255),
                score int,
                pb varchar(255) PRIMARY KEY,
                tipo varchar(255),
                cruce varchar(255),
                geometry geometry,
            )
            """
        )
        conn.commit()

        # insertar datos
    contador = 0
    for index, row in puntos_censos_mensuales.iterrows():

        data = cursor.execute(f"SELECT id FROM {tabla_name_3} where id = {index+1}")

        data2 = data.fetchone()

        if data2 == None:

            cursor.execute(
                f"""
                INSERT INTO {tabla_name_3} (x, y, status, score, pb, tipo, cruce, geometry)
                VALUES (?,?,?,?,?, ?, ?, geometry::STGeomFromText(?, 0))
                """,
                row["X"],
                row["Y"],
                row["Status"],
                row["Score"],
                row["PB"],
                row["TIPO"],
                row["CRUCE"],
                row["geometry"].wkt,
            )
            conn.commit()
            contador += 1
            print(f"Insertando fila {contador} de {len(puntos_censos_mensuales)}")
        else:
            print(f"fila {index+1} de {len(puntos_censos_mensuales)} ya existe")

    cursor.close()
    conn.close()


def cargar_volumen_ciclistas_anual():

    try:
        cursor.execute(f"select * from {tabla_name_4}")
    except pyodbc.ProgrammingError:
        cursor.execute(
            f"""
            CREATE TABLE {tabla_name_4} (
                id int IDENTITY(1,1) PRIMARY KEY,
                centroide int,
                cruce varchar(255),
                anio date,
                turno varchar(255),
                cantidad_ciclistas int,
                tipo_relevamiento varchar(255),
                CONSTRAINT fk_centroide FOREIGN KEY (centroide) REFERENCES {tabla_name_2}(centroide),
            );    
            """
        )
        conn.commit()

        # insertar datos
    contador = 0
    for index, row in volumen_ciclista_anual.iterrows():

        data = cursor.execute(f"SELECT id FROM {tabla_name_4} where id = {index+1}")

        data2 = data.fetchone()

        if data2 == None:

            cursor.execute(
                f"""
                INSERT INTO {tabla_name_4} (centroide, cruce, anio, turno, cantidad_ciclistas, tipo_relevamiento)
                VALUES (?,?,?,?, ?, ?)
                """,
                row["centroide"],
                row["cruce"],
                row["año"],
                row["turno"],
                row["cantidad_ciclistas"],
                row["tipo_relevamiento"],
            )
            conn.commit()
            contador += 1
            print(f"Insertando fila {contador} de {len(volumen_ciclista_anual)}")
        else:
            print(f"fila {index+1} de {len(volumen_ciclista_anual)} ya existe")

    cursor.close()
    conn.close()


def cargar_volumen_ciclistas_mensual():

    try:
        cursor.execute(f"select * from {tabla_name_5}")
    except pyodbc.ProgrammingError:
        cursor.execute(
            f"""
            CREATE TABLE {tabla_name_5} (
                id int IDENTITY(1,1) PRIMARY KEY,
                anioo date,
                mes varchar(255),
                punto_cruce varchar(255),
                punto_referencia varchar(255),
                cantidad_ciclistas int,
            );    
            """
        )
        conn.commit()

        # insertar datos
    contador = 0
    for index, row in volumen_ciclista_mensual.iterrows():

        data = cursor.execute(f"SELECT id FROM {tabla_name_5} where id = {index+1}")

        data2 = data.fetchone()

        if data2 == None:

            cursor.execute(
                f"""
                INSERT INTO {tabla_name_5} (anioo, mes, punto_cruce, punto_referencia, cantidad_ciclistas)
                VALUES (?,?,?,?, ?)
                """,
                row["año"],
                row["mes"],
                row["punto_cruce"],
                row["punto_referencia"],
                row["cantidad_ciclistas"],
            )
            conn.commit()
            contador += 1
            print(f"Insertando fila {contador} de {len(volumen_ciclista_mensual)}")
        else:
            print(f"fila {index+1} de {len(volumen_ciclista_mensual)} ya existe")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    cargar_datos_ciclovias()
    cargar_puntos_censos_anuales()
    cargar_puntos_censos_mensuales()
    cargar_volumen_ciclistas_anual()
    cargar_volumen_ciclistas_mensual()
