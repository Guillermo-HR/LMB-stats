# Script para obtener datos de la LMB y guardarlos en una base de datos PostgreSQL
# Si ya esxisten juegos actualiza solo los juegos pendientes

import requests
import json
from datetime import datetime
import polars as pl
import psycopg2
import dotenv
import os

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

def getJugadoresRegistrados():
    query = """SELECT jugador_id FROM jugador"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        jugadores = cursor.fetchall()
    return set([jugador[0] for jugador in jugadores])

def getUmpiresRegistrados():
    query = """SELECT umpire_id FROM umpire"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        umpires = cursor.fetchall()
    return set([umpire[0] for umpire in umpires])

def getEstadiosRegistrados():
    query = """SELECT estadio_id FROM estadio"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        estadios = cursor.fetchall()
    return set([estadio[0] for estadio in estadios])

def getEquiposRegistrados():
    query = """SELECT equipo_id FROM equipo"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        equipos = cursor.fetchall()
    return set([equipo[0] for equipo in equipos])

def getTurno_idActual():
    query = """SELECT MAX(turno_id) 
                FROM turno"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        turno_id = cursor.fetchone()[0]
    if turno_id is None:
        turno_id = 0
    return turno_id

jugadoresRegistrados = getJugadoresRegistrados() # Para evitar busquedas en la base para validar cada jugador
umpiresRegistrados = getUmpiresRegistrados() # Para evitar busquedas en la base para validar cada umpire
estadiosRegistrados = getEstadiosRegistrados() # Para evitar busquedas en la base para validar cada estadio
equiposRegistrados = getEquiposRegistrados() # Para filtrar solo los juegos de los equipos de la liga

turno_id = getTurno_idActual()

def agregarDatosTablaPosicion():
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

    df = pl.DataFrame(posiciones, schema=schema_df_posiciones)
    df.write_database(
        table_name='posicion',
        connection=connection_uri,
        if_table_exists='append'
    )

def agregarDatosTablaTipo_juego():
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

    df = pl.DataFrame(tipo_juegos, schema=schema_df_tipo_juegos)
    df.write_database(
        table_name='tipo_juego',
        connection=connection_uri,
        if_table_exists='append'
    )

def agregarDatosTablaStatus_juego():
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

    df = pl.DataFrame(status_juegos, schema=schema_df_status_juegos)
    df.write_database(
        table_name='status_juego',
        connection=connection_uri,
        if_table_exists='append'
    )

def agregarDatosTablaTipo_turno():
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

    df = pl.DataFrame(tipo_turnos, schema=schema_df_tipo_turno)
    df.write_database(
        table_name='tipo_turno',
        connection=connection_uri,
        if_table_exists='append'
    )

def agregarDatosTipo_lanzamiento():
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

    df = pl.DataFrame(tipo_lanzamientos, schema=schema_df_tipo_lanzamiento)

    df.write_database(
        table_name='tipo_lanzamiento',
        connection=connection_uri,
        if_table_exists='append'
    )

def agregarDatosEquipo():
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

    # Agregar datos de Mariaches de Guadalajara (no aparence en la API)Add commentMore actions
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

    df = pl.DataFrame(equipos, schema=schema_df_equipo)

    df.write_database(
        table_name='equipo',
        connection=connection_uri,
        if_table_exists='append'
    )

def validarTablasIndependientes():
    query = """SELECT COUNT(*)=0
               FROM {}"""
    
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        
        # Verificar si la tabla posicion esta vacia
        cursor.execute(query.format('posicion'))
        if cursor.fetchone()[0]:
            agregarDatosTablaPosicion()

        # Verificar si la tabla tipo_juego esta vacia
        cursor.execute(query.format('tipo_juego'))
        if cursor.fetchone()[0]:
            agregarDatosTablaTipo_juego()
        
        # Verificar si la tabla status_juego esta vacia
        cursor.execute(query.format('status_juego'))
        if cursor.fetchone()[0]:
            agregarDatosTablaStatus_juego()
        
        # Verificar si la tabla tipo_turno esta vacia
        cursor.execute(query.format('tipo_turno'))
        if cursor.fetchone()[0]:
            agregarDatosTablaTipo_turno()

        # Verificar si la tabla tipo_lanzamiento esta vacia
        cursor.execute(query.format('tipo_lanzamiento'))
        if cursor.fetchone()[0]:
            agregarDatosTipo_lanzamiento()

        # Verificar si la tabla equipo esta vacia
        cursor.execute(query.format('equipo'))
        if cursor.fetchone()[0]:
            agregarDatosEquipo()
    
def getUltimoPartidoRegistrado():
    query = """SELECT (MAX(DATE_TRUNC('day',primer_lanzamiento)::date ))
                FROM juego"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        ultimoPartidoRegistrado = cursor.fetchone()[0]
    if ultimoPartidoRegistrado is None:
        ultimoPartidoRegistrado = datetime(2021, 1, 1).date()
    return ultimoPartidoRegistrado

def getClavesJuegosUltimoDiaRegistrado():
    query = """SELECT juego_id FROM juego
                WHERE DATE_TRUNC('day', primer_lanzamiento) = (SELECT MAX(DATE_TRUNC('day', primer_lanzamiento)) 
                FROM juego)"""
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        juegos = cursor.fetchall()
    return set([juego[0] for juego in juegos])

def getClavesJuegosTemporada(temporada):
    juegosTemporada = requests.get(urlBaseV1 + f'schedule?sportId=23&leageId=125&season={temporada}').content
    juegosTemporada = json.loads(juegosTemporada)
    juegosTemporada = juegosTemporada['dates']

    ultimoPartidoRegistrado = getUltimoPartidoRegistrado().strftime('%Y-%m-%d')
    diaActual = datetime.now().strftime('%Y-%m-%d')
    clavesJuegosUltimoDiaRegistrado = getClavesJuegosUltimoDiaRegistrado()
        
    clavesJuegosTemporada = []
    for dia in juegosTemporada:
        if dia['date'] < ultimoPartidoRegistrado:
            continue
        if dia['date'] > diaActual:
            break
        for juego in dia['games']:
            local_id = juego['teams']['home']['team']['id']
            visitante_id = juego['teams']['away']['team']['id']
            juego_id = juego['gamePk']
            codedGameState = juego['status']['codedGameState']

            if juego_id in clavesJuegosUltimoDiaRegistrado:
                continue

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

def insertarDatosTablaTurno(datosTablaTurno):
    if datosTablaTurno.is_empty():
        return
    datosTablaTurno.write_database(
        table_name='turno',
        connection=connection_uri,
        if_table_exists='append'
    )

def insertarDatosTablaLanzamiento(datosTablaLanzamiento):
    if datosTablaLanzamiento.is_empty():
        return
    datosTablaLanzamiento.write_database(
        table_name='lanzamiento',
        connection=connection_uri,
        if_table_exists='append'
    )

def procesarTurnos(datosJuegoRaw):
    pitchers_local = []
    pitchers_visitante = []

    juego_id = int(datosJuegoRaw['gameData']['game']['pk'])

    datosTablaTurno = {
        'turno_id': [],
        'at_bat_descripcion': [],
        'entrada': [],
        'es_parte_alta': [],
        'cuenta_outs': [],
        'carreras_anotadas': [],
        'juego_id': [],
        'bateador_id': [],
        'pitcher_id': [],
        'tipo_turno_id': []
    }
    datosTablaLanzamiento = {
        'turno_id': [],
        'numero_lanzamiento': [],
        'es_jugada': [],
        'es_bola': [],
        'es_strike': [],
        'es_foul': [],
        'es_out': [],
        'cuenta_bolas': [],
        'cuenta_strikes': [],
        'x': [],
        'y': [],
        'tipo_lanzamiento_id': []
    }
    
    contador_outs = 0
    contador_carreras_local = 0
    contador_carreras_visitante = 0

    for jugada in datosJuegoRaw['liveData']['plays']['allPlays']:
        # Validar que tenga informacion
        if 'eventType' not in jugada['result']:
            continue
        # Procesar turno
        global turno_id
        turno_id += 1
        at_bat_descripcion = str(jugada['result']['description'])
        entrada = int(jugada['about']['inning'])
        es_parte_alta = bool(jugada['about']['isTopInning'])
        cuenta_outs = contador_outs
        contador_outs = int(jugada['count']['outs'])
        if cuenta_outs == 3:
            contador_outs = 0
        marcador_local = int(jugada['result']['homeScore'])
        marcador_visitante = int(jugada['result']['awayScore'])
        if es_parte_alta:
            carreras_anotadas = marcador_visitante - contador_carreras_visitante
            contador_carreras_visitante = marcador_visitante
        else:
            carreras_anotadas = marcador_local - contador_carreras_local
            contador_carreras_local = marcador_local
        
        bateador_id = int(jugada['matchup']['batter']['id'])
        pitcher_id = int(jugada['matchup']['pitcher']['id'])
        tipo_turno_id = str(jugada['result']['eventType'])

        datosTablaTurno['turno_id'].append(turno_id)
        datosTablaTurno['at_bat_descripcion'].append(at_bat_descripcion)
        datosTablaTurno['entrada'].append(entrada)
        datosTablaTurno['es_parte_alta'].append(es_parte_alta)
        datosTablaTurno['cuenta_outs'].append(cuenta_outs)
        datosTablaTurno['carreras_anotadas'].append(carreras_anotadas)
        datosTablaTurno['juego_id'].append(juego_id)

        datosTablaTurno['bateador_id'].append(bateador_id)
        datosTablaTurno['pitcher_id'].append(pitcher_id)
        datosTablaTurno['tipo_turno_id'].append(tipo_turno_id)

        if es_parte_alta and pitcher_id not in pitchers_local:
            pitchers_local.append(pitcher_id)
        elif not es_parte_alta and pitcher_id not in pitchers_visitante:
            pitchers_visitante.append(pitcher_id)

        # Procesar lanzamiento
        contador_bolas = 0
        contador_strikes = 0
        numero_lanzamiento = 0
        for lanzamiento in jugada['playEvents']:
            if 'eventType' in lanzamiento['details']:
                continue

            numero_lanzamiento += 1
            if 'isInPlay' in lanzamiento['details']:
                es_jugada = bool(lanzamiento['details']['isInPlay'])
            else:
                es_jugada = False
            if 'isBall' in lanzamiento['details']:
                es_bola = bool(lanzamiento['details']['isBall'])
            else:
                es_bola = False
            if 'isStrike' in lanzamiento['details']:
                es_strike = bool(lanzamiento['details']['isStrike'])
            else:
                es_strike = False
            es_out = bool(lanzamiento['details']['isOut'])
            if contador_strikes == 2 and es_strike and not es_out:
                es_foul = True
            else:
                es_foul = False
            cuenta_bolas = contador_bolas
            contador_bolas = int(lanzamiento['count']['balls'])
            cuenta_strikes = contador_strikes
            contador_strikes = int(lanzamiento['count']['strikes'])
            if 'pitchData' in lanzamiento and 'x' in lanzamiento['pitchData']['coordinates'] and 'y' in lanzamiento['pitchData']['coordinates']:
                x = -0.021 * (lanzamiento['pitchData']['coordinates']['x']) + 2.298
                y = -0.021 * (lanzamiento['pitchData']['coordinates']['y']) + 5.803
            else:
                x = None
                y = None
                print(f'lanzamiento: {turno_id}-{numero_lanzamiento} -> faltan datos de coordenadas')
            tipo_lanzamiento_id = str(lanzamiento['details']['code'])

            datosTablaLanzamiento['turno_id'].append(turno_id)
            datosTablaLanzamiento['numero_lanzamiento'].append(numero_lanzamiento)
            datosTablaLanzamiento['es_jugada'].append(es_jugada)
            datosTablaLanzamiento['es_bola'].append(es_bola)
            datosTablaLanzamiento['es_strike'].append(es_strike)
            datosTablaLanzamiento['es_foul'].append(es_foul)
            datosTablaLanzamiento['es_out'].append(es_out)
            datosTablaLanzamiento['cuenta_bolas'].append(cuenta_bolas)
            datosTablaLanzamiento['cuenta_strikes'].append(cuenta_strikes)
            datosTablaLanzamiento['x'].append(x)
            datosTablaLanzamiento['y'].append(y)
            datosTablaLanzamiento['tipo_lanzamiento_id'].append(tipo_lanzamiento_id)
            
    schema_df_turno = {
        'turno_id': pl.Int64,
        'at_bat_descripcion': pl.String,
        'entrada': pl.Int64,
        'es_parte_alta': pl.Boolean,
        'cuenta_outs': pl.Int64,
        'carreras_anotadas': pl.Int64,
        'juego_id': pl.Int64,
        'bateador_id': pl.Int64,
        'pitcher_id': pl.Int64,
        'tipo_turno_id': pl.String
    }
    insertarDatosTablaTurno(pl.DataFrame(datosTablaTurno, schema=schema_df_turno))

    schema_df_lanzamiento = {
        'turno_id': pl.Int64,
        'numero_lanzamiento': pl.Int64,
        'es_jugada': pl.Boolean,
        'es_bola': pl.Boolean,
        'es_strike': pl.Boolean,
        'es_foul': pl.Boolean,
        'es_out': pl.Boolean,
        'cuenta_bolas': pl.Int64,
        'cuenta_strikes': pl.Int64,
        'x': pl.Float32,
        'y': pl.Float32,
        'tipo_lanzamiento_id': pl.String
    }
    insertarDatosTablaLanzamiento(pl.DataFrame(datosTablaLanzamiento, schema=schema_df_lanzamiento))
    
    return pitchers_visitante, pitchers_local
    
def getDatosTablaPitcher_juego(pitchers, datosJuegoRaw, es_local):
    if es_local:
        datosPitcherRaw = datosJuegoRaw['liveData']['boxscore']['teams']['home']['players']
    else:
        datosPitcherRaw = datosJuegoRaw['liveData']['boxscore']['teams']['away']['players']
    
    datosTablaPitcher_juego = {
        'juego_id': [],
        'pitcher_id': [],
        'es_local': [],
        'es_abridor': [],
        'es_ganador': [],
        'es_perdedor': [],
        'oportunidad_salvamento': [],
        'es_salvamento': [],
        'at_bats': [],
        'carreras': [],
        'carreras_limpias': []
    }
    
    juego_id = int(datosJuegoRaw['gameData']['game']['pk'])
    for i, pitcher in enumerate(pitchers):
        datosPitcher = datosPitcherRaw[f'ID{pitcher}']
        pitcher_id = int(pitcher)
        if i == 0:
            es_abridor = True
        else:
            es_abridor = False
        if datosPitcher['stats']['pitching']['wins'] == 1:
            es_ganador = True
        else:
            es_ganador = False
        if datosPitcher['stats']['pitching']['losses'] == 1:
            es_perdedor = True
        else:
            es_perdedor = False
        if datosPitcher['stats']['pitching']['saveOpportunities'] == 1:
            oportunidad_salvamento = True
        else:
            oportunidad_salvamento = False
        if datosPitcher['stats']['pitching']['saves'] == 1:
            es_salvamento = True
        else:
            es_salvamento = False
        at_bats = int(datosPitcher['stats']['pitching']['atBats'])
        carreras = int(datosPitcher['stats']['pitching']['runs'])
        carreras_limpias = int(datosPitcher['stats']['pitching']['earnedRuns'])

        datosTablaPitcher_juego['juego_id'].append(juego_id)
        datosTablaPitcher_juego['pitcher_id'].append(pitcher_id)
        datosTablaPitcher_juego['es_local'].append(es_local)
        datosTablaPitcher_juego['es_abridor'].append(es_abridor)
        datosTablaPitcher_juego['es_ganador'].append(es_ganador)
        datosTablaPitcher_juego['es_perdedor'].append(es_perdedor)
        datosTablaPitcher_juego['oportunidad_salvamento'].append(oportunidad_salvamento)
        datosTablaPitcher_juego['es_salvamento'].append(es_salvamento)
        datosTablaPitcher_juego['at_bats'].append(at_bats)
        datosTablaPitcher_juego['carreras'].append(carreras)
        datosTablaPitcher_juego['carreras_limpias'].append(carreras_limpias)

    schema_df_pitcher_juego = {
        'juego_id': pl.Int64,
        'pitcher_id': pl.Int64,
        'es_local': pl.Boolean,
        'es_abridor': pl.Boolean,
        'es_ganador': pl.Boolean,
        'es_perdedor': pl.Boolean,
        'oportunidad_salvamento': pl.Boolean,
        'es_salvamento': pl.Boolean,
        'at_bats': pl.Int64,
        'carreras': pl.Int64,
        'carreras_limpias': pl.Int64
    }
    return pl.DataFrame(datosTablaPitcher_juego, schema=schema_df_pitcher_juego)

def insertarDatosTablaPitcher_juego(datosTablaPitcher_juego):
    if datosTablaPitcher_juego.is_empty():
        return
    datosTablaPitcher_juego.write_database(
        table_name='juego_pitcher',
        connection=connection_uri,
        if_table_exists='append'
    )

def elimiarJuego(juego_id):
    query = """DELETE FROM juego WHERE juego_id = %s"""
    datos = [juego_id]
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        cursor.execute(query, datos)
        conn.commit()

def procesarTemporada(temporada):
    clavesJuegosTemporadaorada = getClavesJuegosTemporada(temporada)
    for juego_id in clavesJuegosTemporadaorada:
        try:
            datosJuegoRaw = getDatosJuegoRaw(juego_id)
            if datosJuegoRaw is None:
                continue
            datosTablaJuego = getDatosTablaJuego(datosJuegoRaw)
            FkTablaJuego = {
                'estadio_id': datosTablaJuego['estadio_id'],
                'umpire_home_id': datosTablaJuego['umpire_home_id'],
                'umpire_1b_id': datosTablaJuego['umpire_1b_id'],
                'umpire_2b_id': datosTablaJuego['umpire_2b_id'],
                'umpire_3b_id': datosTablaJuego['umpire_3b_id']
            }
            validarFkTablaJuego(FkTablaJuego, datosJuegoRaw)
            insertDatosTablaJuego(datosTablaJuego)
            datosTablaJugador = getDatosTablaJugador(datosJuegoRaw['gameData']['players'], juego_id)
            insertarDatosTablaJugador(datosTablaJugador)
            pitchers_visitante, pitchers_local = procesarTurnos(datosJuegoRaw)
            datosTablaPitcher_juego_visitante = getDatosTablaPitcher_juego(pitchers_visitante, datosJuegoRaw, False)
            insertarDatosTablaPitcher_juego(datosTablaPitcher_juego_visitante)
            datosTablaPitcher_juego_local = getDatosTablaPitcher_juego(pitchers_local, datosJuegoRaw, True)
            insertarDatosTablaPitcher_juego(datosTablaPitcher_juego_local)
        except Exception as err:
            elimiarJuego(juego_id)
            raise err
    
def main():
    temporadas = [2025] #! Esto solo es para las pruebas
    temporadas = list(range(2021, 2026)) 

    validarTablasIndependientes()

    for temporada in temporadas:
        procesarTemporada(temporada)

def limpiarTablas():
    query = """DELETE FROM {}"""
    tablas = ['juego_pitcher', 'lanzamiento', 'tipo_lanzamiento', 'turno', 'tipo_turno', 'jugador', 'posicion', 'juego',
              'equipo', 'tipo_juego', 'estadio', 'status_juego', 'umpire']
    with psycopg2.connect(**connection) as conn:
        cursor = conn.cursor()
        for tabla in tablas:
            cursor.execute(query.format(tabla))
        conn.commit()

if __name__ == '__main__':
    #limpiarTablas()  #!Solo descomentar si se quiere reiniciar las tablas
    main()