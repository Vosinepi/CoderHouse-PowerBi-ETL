from variables import nombre_base_datos


# credenciales base de datos

# SQL-server destino de datos
base_de_datos_config = {
    "trusted_connection": "yes",
    "driver": "SQL Server",
    "server": "DESKTOP-AJU1HOS\SQLEXPRESS",
    "database": "{}".format(nombre_base_datos),
}
