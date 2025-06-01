# Script para obtener datos de la LMB y guardarlos en una base de datos PostgreSQL
import requests
import json
from datetime import datetime
import polars as pl
import psycopg2
import dotenv
import os

jugadoresRegistrados = set() # Para evitar busquedas en la base para validar cada jugador
umpiresRegistrados = set() # Para evitar busquedas en la base para validar cada umpire
estadiosRegistrados = set() # Para evitar busquedas en la base para validar cada estadio
equiposRegistrados = set() # Para filtrar solo los juegos de los equipos de la liga

# Datos conexion a la base de datos
dotenv.load_dotenv()
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
connection = {
    'host': host,
    'port': port,
    'database': database,
    'user': user,
    'password': password
    }
connection_uri = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

# URLs de la API de MLB
urlBaseV1 = f'https://statsapi.mlb.com/api/v1/'
urlBaseV1_1 = 'https://statsapi.mlb.com/api/v1.1/'

def limpiarTablas():
    query = """DELETE FROM {}"""
    tablas = ['juego_pitcher', 'lanzamiento', 'tipo_lanzamiento', 'turno', 'tipo_turno', 'jugador', 'posicion', 'juego',
              'equipo', 'tipo_juego', 'estadio', 'status_juego', 'umpire']
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        for tabla in tablas:
            cursor.execute(query.format(tabla))
        conn.commit()

def getDatosTablaPosicion():
    datosPosicionesRaw = requests.get(urlBaseV1 + 'positions').content
    datosPosicionesRaw = json.loads(datosPosicionesRaw)
    
    posiciones = {'posicion_id': [], 'descripcion': []}
    for posicion in datosPosicionesRaw:
        try:
            indice_posicion_id = posiciones['posicion_id'].index(posicion['code'])
            posiciones['descripcion'][indice_posicion_id] += f'/{posicion["fullName"]}'
        except ValueError:
            posiciones['posicion_id'].append(posicion['code'])
            posiciones['descripcion'].append(posicion['fullName'])
    
    schema_df_posiciones = {
        'posicion_id': pl.String,
        'descripcion': pl.String,
    }
    
    return pl.DataFrame(posiciones, schema=schema_df_posiciones)

def insertarDatosTablaPosicion(datosTablaPosicion):
    datosTablaPosicion.write_database(
        table_name='posicion',
        connection=connection_uri,
        if_table_exists='append'
    )
    return 1

def getDatosTablaTipo_juego():
    datosTipo_juegoRaw = requests.get(urlBaseV1 + 'gameTypes').content
    datosTipo_juegoRaw = json.loads(datosTipo_juegoRaw)
    
    tipo_juegos = {'tipo_juego_id': [], 'descripcion': []}
    for tipo_juego in datosTipo_juegoRaw:
        try:
            indice_tipo_juego_id = tipo_juegos['tipo_juego_id'].index(tipo_juego['id'])
            tipo_juegos['descripcion'][indice_tipo_juego_id] += f'/{tipo_juego["description"]}'
        except ValueError:
            tipo_juegos['tipo_juego_id'].append(tipo_juego['id'])
            tipo_juegos['descripcion'].append(tipo_juego['description'])

    schema_df_tipo_juegos = {
        'tipo_juego_id': pl.String,
        'descripcion': pl.String,
    }
    
    return pl.DataFrame(tipo_juegos, schema=schema_df_tipo_juegos)

def insertarDatosTablaTipo_juego(datosTablaTipo_juego):
    datosTablaTipo_juego.write_database(
        table_name='tipo_juego',
        connection=connection_uri,
        if_table_exists='append'
    )

def getDatosTablaStatus_juego():
    datosStatus_juegoRaw = requests.get(urlBaseV1 + 'gameStatus').content
    datosStatus_juegoRaw = json.loads(datosStatus_juegoRaw)
    
    status_juegos = {'status_juego_id': [], 'descripcion': []}
    for status_juego in datosStatus_juegoRaw:
        try:
            indice_status_juego_id = status_juegos['status_juego_id'].index(status_juego['statusCode'])
            status_juegos['descripcion'][indice_status_juego_id] += f'/{status_juego["detailedState"]}'
        except ValueError:
            status_juegos['status_juego_id'].append(status_juego['statusCode'])
            status_juegos['descripcion'].append(status_juego['detailedState'])
    
    schema_df_status_juegos = {
        'status_juego_id': pl.String,
        'descripcion': pl.String,
    }
    
    return pl.DataFrame(status_juegos, schema=schema_df_status_juegos)

def insertarDatosTablaStatus_juego(datosTablaStatus_juego):
    datosTablaStatus_juego.write_database(
        table_name='status_juego',
        connection=connection_uri,
        if_table_exists='append'
    )

def getDatosTablaTipo_turno():
    datosTipo_turnoRaw = requests.get(urlBaseV1 + 'eventTypes').content
    datosTipo_turnoRaw = json.loads(datosTipo_turnoRaw)
    
    tipo_turnos = {'tipo_turno_id': [], 'descripcion': []}
    for tipo_turno in datosTipo_turnoRaw:
        try:
            indice_tipo_turno_id = tipo_turnos['tipo_turno_id'].index(tipo_turno['code'])
            tipo_turnos['descripcion'][indice_tipo_turno_id] += f'/{tipo_turno["description"]}'
        except ValueError:
            tipo_turnos['tipo_turno_id'].append(tipo_turno['code'])
            tipo_turnos['descripcion'].append(tipo_turno['description'])
    
    schema_df_tipo_turno = {
        'tipo_turno_id': pl.String,
        'descripcion': pl.String,
    }
    
    return pl.DataFrame(tipo_turnos, schema=schema_df_tipo_turno)

def insertarDatosTipo_turno(datosTablaTipo_turno):
    datosTablaTipo_turno.write_database(
        table_name='tipo_turno',
        connection=connection_uri,
        if_table_exists='append'
    )

def getDatosTablaTipo_lanzamiento():
    datosTipo_lanzamientoRaw = requests.get(urlBaseV1 + 'pitchCodes').content
    datosTipo_lanzamientoRaw = json.loads(datosTipo_lanzamientoRaw)
    
    tipo_lanzamientos = {'tipo_lanzamiento_id': [], 'descripcion': []}
    for tipo_lanzamiento in datosTipo_lanzamientoRaw:
        try:
            indice_tipo_lanzamiento_id = tipo_lanzamientos['tipo_lanzamiento_id'].index(tipo_lanzamiento['code'])
            tipo_lanzamientos['descripcion'][indice_tipo_lanzamiento_id] += f'/{tipo_lanzamiento["description"]}'
        except ValueError:
            tipo_lanzamientos['tipo_lanzamiento_id'].append(tipo_lanzamiento['code'])
            tipo_lanzamientos['descripcion'].append(tipo_lanzamiento['description'])
    
    schema_df_tipo_lanzamiento = {
        'tipo_lanzamiento_id': pl.String,
        'descripcion': pl.String,
    }
    
    return pl.DataFrame(tipo_lanzamientos, schema=schema_df_tipo_lanzamiento)

def insertarDatosTipo_lanzamiento(datosTablaTipo_lanzamiento):
    datosTablaTipo_lanzamiento.write_database(
        table_name='tipo_lanzamiento',
        connection=connection_uri,
        if_table_exists='append'
    )

def getDatosTablaEquipo():
    datosEquipoRaw = requests.get(urlBaseV1 + 'teams?leagueId=125').content
    datosEquipoRaw = json.loads(datosEquipoRaw)['teams']
    
    equipos = {'equipo_id': [], 'nombre': [], 'abreviacion': [], 'zona': []}
    for equipo in datosEquipoRaw:
        equipo_id = int(equipo['id'])
        nombre = str(equipo['name'])
        abreviacion = str(equipo['abbreviation'])
        zona = str(equipo['division']['name'][15:])

        equipos['equipo_id'].append(equipo_id)
        equipos['nombre'].append(nombre)
        equipos['abreviacion'].append(abreviacion)
        equipos['zona'].append(zona)

    # Agregar datos de Mariaches de Guadalajara (no aparence en la API)
    equipos['equipo_id'].append(5566)
    equipos['nombre'].append('Mariachis de Guadalajara')
    equipos['abreviacion'].append('GDL')
    equipos['zona'].append('Norte')
    
    schema_df_equipo = {
        'equipo_id': pl.Int64,
        'nombre': pl.String,
        'abreviacion': pl.String,
        'zona': pl.String
    }
    
    return pl.DataFrame(equipos, schema=schema_df_equipo)

def insertarDatosEquipo(datosTablaEquipo):
    datosTablaEquipo.write_database(
        table_name='equipo',
        connection=connection_uri,
        if_table_exists='append'
    )
    global equiposRegistrados
    equiposRegistrados = set(datosTablaEquipo['equipo_id'].to_list())

def prepararBaseDatos():
    limpiarTablas()
    datosTablaPosicion = getDatosTablaPosicion()
    insertarDatosTablaPosicion(datosTablaPosicion)
    datosTablaTipo_juego = getDatosTablaTipo_juego()
    insertarDatosTablaTipo_juego(datosTablaTipo_juego)
    datosTablaStatus_juego = getDatosTablaStatus_juego()
    insertarDatosTablaStatus_juego(datosTablaStatus_juego)
    datosTablaTipo_turno = getDatosTablaTipo_turno()
    insertarDatosTipo_turno(datosTablaTipo_turno)
    datosTablaTipo_lanzamiento = getDatosTablaTipo_lanzamiento()
    insertarDatosTipo_lanzamiento(datosTablaTipo_lanzamiento)
    datosTablaEquipo = getDatosTablaEquipo()
    insertarDatosEquipo(datosTablaEquipo)

def getClavesJuegosTemporada(temporada):
    juegosTemporada = requests.get(urlBaseV1 + f'schedule?sportId=23&leageId=125&season={temporada}').content
    juegosTemporada = json.loads(juegosTemporada)
    juegosTemporada = juegosTemporada['dates']

    diaActual = datetime.now().strftime('%Y-%m-%d')
        
    clavesJuegosTemporada = []
    for dia in juegosTemporada:
        if dia['date'] >= diaActual:
            break
        for juego in dia['games']:
            local_id = juego['teams']['home']['team']['id']
            visitante_id = juego['teams']['away']['team']['id']
            juego_id = juego['gamePk']
            codedGameState = juego['status']['codedGameState']

            if (local_id not in equiposRegistrados) or (visitante_id not in equiposRegistrados):
                continue
            
            if juego_id in clavesJuegosTemporada:
                continue
                
            if codedGameState != 'F':
                continue

            clavesJuegosTemporada.append(juego['gamePk'])
    
    return clavesJuegosTemporada

def getDatosJuegoRaw(juego_id):
    datosJuegoRaw = requests.get(urlBaseV1_1 + f'game/{juego_id}/feed/live').content
    datosJuegoRaw = json.loads(datosJuegoRaw)

    if 'error' in datosJuegoRaw:
        raise ValueError(f'Error al obtener datos del juego {juego_id}. Status {datosJuegoRaw['status']}: {datosJuegoRaw['error']}')
    
    if datosJuegoRaw['gameData']['status']['codedGameState'] in ['D', 'C']:
        return None

    return datosJuegoRaw

def getDatosTablaJuego(datosJuegoRaw):
    datosTablaJuego = {}

    juego_id = int(datosJuegoRaw['gameData']['game']['pk'])
    temporada = str(datosJuegoRaw['gameData']['game']['season'])

    primer_lanzamiento = datosJuegoRaw['gameData']['gameInfo']['firstPitch']
    duracion = int(datosJuegoRaw['gameData']['gameInfo']['gameDurationMinutes'])
    if 'delayDurationMinutes' in datosJuegoRaw['gameData']['gameInfo']:
        retraso = int(datosJuegoRaw['gameData']['gameInfo']['delayDurationMinutes'])
    else:
        retraso = 0
    numero_entradas = int(datosJuegoRaw['liveData']['linescore']['currentInning'])
    temperatura = int((float(datosJuegoRaw['gameData']['weather']['temp']) - 32) * 5/9)
    viento = str(datosJuegoRaw['gameData']['weather']['wind'])
    asistencia = int(datosJuegoRaw['gameData']['gameInfo']['attendance'])
    carreras_local = int(datosJuegoRaw['liveData']['linescore']['teams']['home']['runs'])
    carreras_visitante = int(datosJuegoRaw['liveData']['linescore']['teams']['away']['runs'])
    gano_local = bool(carreras_local > carreras_visitante)
    umpire_home_id = None
    umpire_1b_id = None
    umpire_2b_id = None
    umpire_3b_id = None
    if len(datosJuegoRaw['liveData']['boxscore']['officials']) != 4:
        print(f'juego: {juego_id} -> faltan umpires')
    for umpire in datosJuegoRaw['liveData']['boxscore']['officials']:
        if umpire['officialType'] == 'Home Plate':
            umpire_home_id = int(umpire['official']['id'])
        elif umpire['officialType'] == 'First Base':
            umpire_1b_id = int(umpire['official']['id'])
        elif umpire['officialType'] == 'Second Base':
            umpire_2b_id = int(umpire['official']['id'])
        elif umpire['officialType'] == 'Third Base':
            umpire_3b_id = int(umpire['official']['id'])

    local_id = int(datosJuegoRaw['gameData']['teams']['home']['id'])
    visitante_id = int(datosJuegoRaw['gameData']['teams']['away']['id'])
    tipo_juego_id = str(datosJuegoRaw['gameData']['game']['type'])
    estadio_id = int(datosJuegoRaw['gameData']['venue']['id'])
    status_juego_id = str(datosJuegoRaw['gameData']['status']['statusCode'])

    datosTablaJuego['juego_id'] = juego_id
    datosTablaJuego['temporada'] = temporada
    datosTablaJuego['primer_lanzamiento'] = primer_lanzamiento
    datosTablaJuego['duracion'] = duracion
    datosTablaJuego['retraso'] = retraso
    datosTablaJuego['numero_entradas'] = numero_entradas
    datosTablaJuego['temperatura'] = temperatura
    datosTablaJuego['viento'] = viento
    datosTablaJuego['asistencia'] = asistencia
    datosTablaJuego['gano_local'] = gano_local

    datosTablaJuego['local_id'] = local_id
    datosTablaJuego['visitante_id'] = visitante_id
    datosTablaJuego['tipo_juego_id'] = tipo_juego_id
    datosTablaJuego['estadio_id'] = estadio_id
    datosTablaJuego['status_juego_id'] = status_juego_id
    datosTablaJuego['umpire_home_id'] = umpire_home_id
    datosTablaJuego['umpire_1b_id'] = umpire_1b_id
    datosTablaJuego['umpire_2b_id'] = umpire_2b_id
    datosTablaJuego['umpire_3b_id'] = umpire_3b_id

    return datosTablaJuego

def validarFkTablaJuego(FkTablaJuego, datosJuegoRaw):
    query_estadio = """INSERT INTO estadio
                    (
                        estadio_id, nombre, ciudad, capacidad, 
                        tipo_pasto, jardin_izquierdo, jardin_central, jardin_derecho
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    query_umpire = """INSERT INTO umpire
                    (
                        umpire_id, nombre
                    )
                    VALUES (%s, %s)"""
    
    if FkTablaJuego['estadio_id'] not in estadiosRegistrados:
        estadiosRegistrados.add(FkTablaJuego['estadio_id'])
        nombre_estadio = str(datosJuegoRaw['gameData']['venue']['name'])
        ciudad_estadio = str(datosJuegoRaw['gameData']['venue']['location']['city'])
        capacidad_estadio = int(datosJuegoRaw['gameData']['venue']['fieldInfo']['capacity'])
        tipo_pasto = str(datosJuegoRaw['gameData']['venue']['fieldInfo']['turfType'])
        jardin_izquierdo = None
        jardin_central = None
        jardin_derecho = None
        if 'leftLine' in datosJuegoRaw['gameData']['venue']['fieldInfo']:
            jardin_izquierdo = int(datosJuegoRaw['gameData']['venue']['fieldInfo']['leftLine'])
        if 'center' in datosJuegoRaw['gameData']['venue']['fieldInfo']:
            jardin_central = int(datosJuegoRaw['gameData']['venue']['fieldInfo']['center'])
        if 'rightLine' in datosJuegoRaw['gameData']['venue']['fieldInfo']:
            jardin_derecho = int(datosJuegoRaw['gameData']['venue']['fieldInfo']['rightLine'])

        possiblesNone = [jardin_izquierdo, jardin_central, jardin_derecho]
        if any(v is None for v in possiblesNone):
            print(f'juego: {FkTablaJuego["estadio_id"]} -> faltan datos del estadio')

        datos = [FkTablaJuego['estadio_id'], nombre_estadio, ciudad_estadio, capacidad_estadio, tipo_pasto, jardin_izquierdo, jardin_central, jardin_derecho]
        with psycopg2.connect(**connection) as conn:
            cursor = conn.cursor()
            cursor.execute(query_estadio, datos)
            conn.commit()
    
    for umpire_id in [FkTablaJuego['umpire_home_id'], FkTablaJuego['umpire_1b_id'], FkTablaJuego['umpire_2b_id'], FkTablaJuego['umpire_3b_id']]:
        if umpire_id is None:
            continue
        if umpire_id in umpiresRegistrados:
            continue
        umpiresRegistrados.add(umpire_id)
        datosUmpireRaw = requests.get(urlBaseV1 + f'people/{umpire_id}').content
        datosUmpireRaw = json.loads(datosUmpireRaw)
        nombre_umpire = str(datosUmpireRaw['people'][0]['fullName'])
        datos = [umpire_id, nombre_umpire]
        with psycopg2.connect(**connection) as conn:
            cursor = conn.cursor()
            cursor.execute(query_umpire, datos)
            conn.commit()

def insertDatosTablaJuego(datosTablaJuego):
    query = """INSERT INTO juego 
                (
                    juego_id, temporada, primer_lanzamiento, duracion, retraso, numero_entradas, temperatura, viento, asistencia, gano_local,
                    local_id, visitante_id, tipo_juego_id, estadio_id, status_juego_id, umpire_home_id, umpire_1b_id, umpire_2b_id, umpire_3b_id
                ) 
                VALUES 
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )"""
    datos = [v for v in datosTablaJuego.values()]
    
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query, datos)
        conn.commit()

def getDatosTablaJugador(datosJugadoresRaw, juego_id):
    jugadores = []
    for jugador in datosJugadoresRaw.values():
        jugador_id = int(jugador['id'])
        
        if jugador_id in jugadoresRegistrados:
            continue
        
        jugadoresRegistrados.add(jugador_id)
        nombre = None
        if 'fullName' in jugador:
            nombre = str(jugador['fullName'])
        
        fecha_nacimiento = None
        if 'birthDate' in jugador:
            fecha_nacimiento = str(jugador['birthDate'])
            fecha_nacimiento = datetime.strptime(fecha_nacimiento, "%Y-%m-%d").date()
        
        pais_nacimiento = None
        if 'birthCountry' in jugador:
            pais_nacimiento = str(jugador['birthCountry'])
        
        lado_bateo = None
        lado_lanzamiento = None
        if 'batSide' in jugador:
            lado_bateo = str(jugador['batSide']['code'])
        if 'pitchHand' in jugador:
            lado_lanzamiento = str(jugador['pitchHand']['code'])
        
        zona_strike_top = None
        zona_strike_bottom = None
        if 'strikeZoneTop'  in jugador:
            zona_strike_top = float(jugador['strikeZoneTop'])
        if 'strikeZoneBottom'  in jugador:
            zona_strike_bottom = float(jugador['strikeZoneBottom'])
        
        posicion_id = None
        if 'primaryPosition' in jugador:
            posicion_id = str(jugador['primaryPosition']['code'])
        
        posiblesNone = [fecha_nacimiento, pais_nacimiento, lado_bateo, lado_lanzamiento, zona_strike_top, zona_strike_bottom, posicion_id]
        if nombre is None:
            print(f'jugador: {jugador_id} -> faltan datos del jugador. Juego: {juego_id}')
        elif any(v is None for v in posiblesNone):
            print(f'jugador: {jugador_id} -> faltan datos del jugador')

        jugadores.append({
            'jugador_id': jugador_id,
            'nombre': nombre,
            'fecha_nacimiento': fecha_nacimiento,
            'pais_nacimiento': pais_nacimiento,
            'lado_bateo': lado_bateo,
            'lado_lanzamiento': lado_lanzamiento,
            'zona_strike_top': zona_strike_top,
            'zona_strike_bottom': zona_strike_bottom,
            'posicion_id': posicion_id
        })

    schema_df_jugadores = {
        'jugador_id': pl.Int64,
        'nombre': pl.String,
        'fecha_nacimiento': pl.Date,
        'pais_nacimiento': pl.String,
        'lado_bateo': pl.String,
        'lado_lanzamiento': pl.String,
        'zona_strike_top': pl.Float32,
        'zona_strike_bottom': pl.Float32,
        'posicion_id': pl.String
    }
    return pl.DataFrame(jugadores, schema=schema_df_jugadores)
        
def insertarDatosTablaJugador(jugadoresFaltantes):
    if jugadoresFaltantes.is_empty():
        return
    jugadoresFaltantes.write_database(
        table_name='jugador',
        connection=connection_uri,
        if_table_exists='append'
    )


def procesarTemporada(temporada, turno_id):
    clavesJuegosTemporadaorada = getClavesJuegosTemporada(temporada)
    for juego_id in clavesJuegosTemporadaorada:
        datosJuegoRaw = getDatosJuegoRaw(juego_id)
        if datosJuegoRaw is None:
            continue
        #! Bloque comentado pq ya esta en la base
        #datosTablaJuego = getDatosTablaJuego(datosJuegoRaw)
        #FkTablaJuego = {
        #    'estadio_id': datosTablaJuego['estadio_id'],
        #    'umpire_home_id': datosTablaJuego['umpire_home_id'],
        #    'umpire_1b_id': datosTablaJuego['umpire_1b_id'],
        #    'umpire_2b_id': datosTablaJuego['umpire_2b_id'],
        #    'umpire_3b_id': datosTablaJuego['umpire_3b_id']
        #}
        #validarFkTablaJuego(FkTablaJuego, datosJuegoRaw)
        #insertDatosTablaJuego(datosTablaJuego)
        #datosTablaJugador = getDatosTablaJugador(datosJuegoRaw['gameData']['players'], juego_id)
        #insertarDatosTablaJugador(datosTablaJugador)
    
def main():
    prepararBaseDatos()

    temporadas = [2025] #! Esto solo es para las pruebas
    temporadas = list(range(2021, 2026)) 
    turno_id = 0

    for temporada in temporadas:
        procesarTemporada(temporada, turno_id)

if __name__ == '__main__':
    main()