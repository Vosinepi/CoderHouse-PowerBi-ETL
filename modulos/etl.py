

from querys import (
    cargar_datos_ciclovias,
    cargar_puntos_censos_anuales,
    cargar_puntos_censos_mensuales,
    cargar_volumen_ciclistas_anual,
    cargar_volumen_ciclistas_mensual,
)



def menu():
    print("1. Cargar datos ciclovias")
    print("2. Cargar Puntos censales anuales")
    print("3. Cargar Puntos censales mensuales")
    print("4. Carga Volumen ciclista anual")
    print("5. Cargar Volumen ciclista mensual")
    print("6. Salir")
    opcion = input("Ingrese una opción: ")

    if opcion == "1":
        cargar_datos_ciclovias()
    elif opcion == "2":
        cargar_puntos_censos_anuales()
    elif opcion == "3":
        cargar_puntos_censos_mensuales()
    elif opcion == "4":
        cargar_volumen_ciclistas_anual()
    elif opcion == "5":
        cargar_volumen_ciclistas_mensual()
    elif opcion == "6":
        exit()
    else:
        print("Opción no válida")
    return opcion


if __name__ == "__main__":
    while True:
        menu()
