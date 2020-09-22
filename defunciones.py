import mysql.connector
import json


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="C4l1f0rn14",
    database="mapa"
)


def consulta(query):
    mycursor = mydb.cursor()
    mycursor.execute(query)
    return mycursor.fetchall()


def mergeDics(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                mergeDics(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
#                if (key != "nombre"):
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def get_in1s():
    query = 'select in1 from defunciones group by in1;'
    return consulta(query)


def covid_muertes_by_in1(in1):
    query = 'select count(residencia_provincia_id) as cantidad from casos where residencia_provincia_id = "' + in1 + '" and fallecido = "SI" and clasificacion_resumen = "CONFIRMADO";'
    return consulta(query)


def covid_muertes_by_in1_argentina():
    query = 'select count(*) as cantidad from casos where fallecido = "SI" and clasificacion_resumen = "CONFIRMADO";'
    return consulta(query)


def provincias():
    lista = {}
    in1s = get_in1s()
    for in1 in in1s:
        cantidad = covid_muertes_by_in1(in1[0])
        new_data = {in1[0]: {"COVID19": cantidad[0][0]}}
        lista = mergeDics(lista, new_data)
        query = 'select causa_id, count(*) cantidad from defunciones where in1 = ' + in1[0] + ' and anio = "2018" group by anio, causa_nombre order by cantidad desc limit 15 ;'
        datos = consulta(query)
        for dato in datos:
            causa = dato[0]
            cantidad = dato[1]
            new_data = {in1[0]: {causa: cantidad}}
            lista = mergeDics(lista, new_data)
    return lista


def argentina():
    cantidad = covid_muertes_by_in1_argentina()
    lista = {"0": {"COVID19": cantidad[0][0]}}
    query = 'select causa_id, count(*) cantidad from defunciones where anio = "2018" group by causa_nombre order by cantidad desc limit 15;'
    for dato in consulta(query):
        newdato = {"0": {dato[0]: dato[1]}}
        lista = mergeDics(lista, newdato)
    return lista


def causas_nombre_id():
    lista = {}
    query = 'select causa_id, causa_nombre from defunciones group by causa_id;'
    for dato in consulta(query):
        lista[dato[0]] = dato[1]
    return lista


def persistir(nombre, datos):
    f = open(nombre, "w")
    f.write(json.dumps(datos, ensure_ascii=False))
    f.close()
    print("Guardado en ", nombre)
    print('>>> ', datos)


data_provincias = provincias()
data_pais = argentina()
data = mergeDics(data_pais, data_provincias)
data["codigos"] = causas_nombre_id()
persistir("defunciones_argentina.json", data)

