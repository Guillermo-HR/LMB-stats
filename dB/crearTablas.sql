-- Archivo para crear las tablas de la base de datos

-- Tablas sin dependencias
DROP TABLE IF EXISTS estadio CASCADE;
CREATE TABLE IF NOT EXISTS estadio
(
    estadio_id      SMALLINT NOT NULL ,
    nombre         TEXT NOT NULL ,
    ciudad         TEXT NOT NULL ,
    capacidad      SMALLINT NOT NULL ,
    tipo_pasto      TEXT NOT NULL ,
    jardin_izquierdo SMALLINT,
    jardin_central  SMALLINT,
    jardin_derecho  SMALLINT,

    PRIMARY KEY (estadio_id)
);

DROP TABLE IF EXISTS tipo_juego CASCADE;
CREATE TABLE IF NOT EXISTS tipo_juego
(
    tipo_juego_id TEXT NOT NULL ,
    descripcion TEXT NOT NULL ,

    PRIMARY KEY (tipo_juego_id)
);

DROP TABLE IF EXISTS equipo CASCADE;
CREATE TABLE IF NOT EXISTS equipo
(
    equipo_id      SMALLINT NOT NULL ,
    nombre         TEXT NOT NULL ,
    abreviacion    TEXT NOT NULL ,
    zona          TEXT NOT NULL ,

    PRIMARY KEY (equipo_id),
    CONSTRAINT equipo_UK_nombre UNIQUE (nombre)
);

DROP TABLE IF EXISTS status_juego CASCADE;
CREATE TABLE IF NOT EXISTS status_juego
(
    status_juego_id      TEXT NOT NULL ,
    descripcion     TEXT NOT NULL ,

    PRIMARY KEY (status_juego_id)
);

DROP TABLE IF EXISTS umpire CASCADE;
CREATE TABLE IF NOT EXISTS umpire
(
    umpire_id      INTEGER NOT NULL ,
    nombre         TEXT NOT NULL ,

    PRIMARY KEY (umpire_id)
);

DROP TABLE IF EXISTS posicion CASCADE;
CREATE TABLE IF NOT EXISTS posicion
(
    posicion_id      TEXT NOT NULL ,
    descripcion     TEXT NOT NULL ,

    PRIMARY KEY (posicion_id)
);

DROP TABLE IF EXISTS tipo_turno CASCADE;
CREATE TABLE IF NOT EXISTS tipo_turno
(
    tipo_turno_id TEXT NOT NULL ,
    descripcion    TEXT NOT NULL ,

    PRIMARY KEY (tipo_turno_id)
);

DROP TABLE IF EXISTS tipo_lanzamiento CASCADE;
CREATE TABLE IF NOT EXISTS tipo_lanzamiento
(
    tipo_lanzamiento_id TEXT NOT NULL ,
    descripcion    TEXT NOT NULL ,

    PRIMARY KEY (tipo_lanzamiento_id)
);

-- Tablas con dependencias
DROP TABLE IF EXISTS jugador CASCADE;
CREATE TABLE IF NOT EXISTS jugador
(
    jugador_id      INTEGER NOT NULL ,
    nombre         TEXT,
    fecha_nacimiento date,
    pais_nacimiento TEXT,
    lado_bateo     TEXT,
    lado_lanzamiento  TEXT,
    zona_strike_top   REAL,
    zona_strike_bottom REAL,

    posicion_id TEXT,

    PRIMARY KEY (jugador_id),
    CONSTRAINT jugador_fk_posicion_id FOREIGN KEY (posicion_id) REFERENCES posicion(posicion_id)
);

DROP TABLE IF EXISTS juego CASCADE;
CREATE TABLE IF NOT EXISTS juego
(
    juego_id      INTEGER NOT NULL ,
    temporada     TEXT NOT NULL ,
    primer_lanzamiento TIMESTAMP(0) WITH TIME ZONE,
    duracion       SMALLINT,
    retraso         SMALLINT,
    numero_entradas SMALLINT,
    temperatura   SMALLINT,
    viento        TEXT,
    asistencia    SMALLINT,
    gano_local   BOOLEAN,

    local_id      SMALLINT NOT NULL ,
    visitante_id   SMALLINT NOT NULL ,
    tipo_juego_id TEXT NOT NULL ,
    estadio_id    SMALLINT NOT NULL ,
    status_juego_id TEXT NOT NULL ,
    umpire_home_id INTEGER,
    umpire_1b_id INTEGER,
    umpire_2b_id INTEGER,
    umpire_3b_id INTEGER,

    PRIMARY KEY (juego_id),
    CONSTRAINT fk_juego_local_id FOREIGN KEY (local_id) REFERENCES equipo(equipo_id),
    CONSTRAINT fk_juego_visitante_id FOREIGN KEY (visitante_id) REFERENCES equipo(equipo_id),
    CONSTRAINT fk_juego_tipo_juego_id FOREIGN KEY (tipo_juego_id) REFERENCES tipo_juego(tipo_juego_id),
    CONSTRAINT fk_juego_estadio_id FOREIGN KEY (estadio_id) REFERENCES estadio(estadio_id),
    CONSTRAINT fk_juego_status_juego_id FOREIGN KEY (status_juego_id) REFERENCES status_juego(status_juego_id),
    CONSTRAINT fk_juego_umpire_home_id FOREIGN KEY (umpire_home_id) REFERENCES umpire(umpire_id),
    CONSTRAINT fk_juego_umpire_1b_id FOREIGN KEY (umpire_1b_id) REFERENCES umpire(umpire_id),
    CONSTRAINT fk_juego_umpire_2b_id FOREIGN KEY (umpire_2b_id) REFERENCES umpire(umpire_id),
    CONSTRAINT fk_juego_umpire_3b_id FOREIGN KEY (umpire_3b_id) REFERENCES umpire(umpire_id)
);

DROP INDEX IF EXISTS idx_juego_local_id;
DROP INDEX IF EXISTS idx_juego_visitante_id;
CREATE INDEX idx_juego_local_id ON juego (local_id);
CREATE INDEX idx_juego_visitante_id ON juego (visitante_id);

DROP TABLE IF EXISTS juego_pitcher CASCADE;
CREATE TABLE IF NOT EXISTS juego_pitcher
(
    juego_id               INTEGER NOT NULL,
    pitcher_id             INTEGER NOT NULL,
    es_local               BOOLEAN NOT NULL,
    es_abridor             BOOLEAN NOT NULL,
    es_ganador             BOOLEAN NOT NULL,
    es_perdedor            BOOLEAN NOT NULL,
    oportunidad_salvamento BOOLEAN NOT NULL,
    es_salvamento          BOOLEAN NOT NULL,
    at_bats                 SMALLINT NOT NULL,
    strike_outs             SMALLINT NOT NULL,
    outs                   SMALLINT NOT NULL,
    balls                  SMALLINT NOT NULL,
    strikes                SMALLINT NOT NULL,
    singles                SMALLINT NOT NULL,
    doubles                SMALLINT NOT NULL,
    triples                SMALLINT NOT NULL,
    home_runs               SMALLINT NOT NULL,
    base_on_balls            SMALLINT NOT NULL,
    intentional_walks       SMALLINT NOT NULL,
    hit_by_pitch             SMALLINT NOT NULL,
    wild_pitches            SMALLINT NOT NULL,
    balks                  SMALLINT NOT NULL,
    runs                   SMALLINT NOT NULL,
    earned_runs             SMALLINT NOT NULL,

    PRIMARY KEY (juego_id, pitcher_id),
    CONSTRAINT fk_juego_pitcher_juego_id FOREIGN KEY (juego_id) REFERENCES juego(juego_id) ON DELETE CASCADE,
    CONSTRAINT fk_juego_pitcher_pitcher_id FOREIGN KEY (pitcher_id) REFERENCES jugador(jugador_id)
);

DROP TABLE IF EXISTS juego_bateador CASCADE;   
CREATE TABLE IF NOT EXISTS juego_bateador
(
    juego_id             INTEGER NOT NULL,
    bateador_id          INTEGER NOT NULL,
    es_local             BOOLEAN NOT NULL,
    at_bats               SMALLINT NOT NULL,
    air_outs              SMALLINT NOT NULL,
    fly_outs              SMALLINT NOT NULL,
    ground_outs           SMALLINT NOT NULL,
    line_outs             SMALLINT NOT NULL,
    pop_outs              SMALLINT NOT NULL,
    strike_outs           SMALLINT NOT NULL,
    ground_into_double_play SMALLINT NOT NULL,
    ground_into_triple_play SMALLINT NOT NULL,
    left_on_base           SMALLINT NOT NULL,
    sac_bunts             SMALLINT NOT NULL,
    sac_flies             SMALLINT NOT NULL,
    singles              SMALLINT NOT NULL,
    doubles              SMALLINT NOT NULL,
    triples              SMALLINT NOT NULL,
    home_runs             SMALLINT NOT NULL,
    base_on_balls          SMALLINT NOT NULL,
    intentional_walks     SMALLINT NOT NULL,
    hit_by_pitch           SMALLINT NOT NULL,
    runs                 SMALLINT NOT NULL,
    rbi                  SMALLINT NOT NULL,
    
    PRIMARY KEY (juego_id, bateador_id),
    CONSTRAINT fk_juego_bateador_bateador_id FOREIGN KEY (bateador_id) REFERENCES jugador(jugador_id),
    CONSTRAINT fk_juego_bateador_juego_id FOREIGN KEY (juego_id) REFERENCES juego(juego_id) ON DELETE CASCADE
);

DROP INDEX IF EXISTS idx_juego_pitcher_pitcher_id;
CREATE INDEX idx_juego_pitcher_pitcher_id ON juego_pitcher (pitcher_id);

DROP TABLE IF EXISTS turno CASCADE;
CREATE TABLE IF NOT EXISTS turno
(
    turno_id      INTEGER NOT NULL ,
    at_bat_descripcion     TEXT,
    entrada        SMALLINT NOT NULL ,
    es_parte_alta      BOOLEAN NOT NULL ,
    cuenta_outs SMALLINT NOT NULL ,
    carreras_anotadas SMALLINT NOT NULL ,

    juego_id      INTEGER NOT NULL ,
    bateador_id   INTEGER NOT NULL ,
    pitcher_id   INTEGER NOT NULL ,
    tipo_turno_id TEXT,

    PRIMARY KEY (turno_id),
    CONSTRAINT fk_turno_juego_id FOREIGN KEY (juego_id) REFERENCES juego(juego_id) ON DELETE CASCADE,
    CONSTRAINT fk_turno_bateador_id FOREIGN KEY (bateador_id) REFERENCES jugador(jugador_id),
    CONSTRAINT fk_turno_pitcher_id FOREIGN KEY (pitcher_id) REFERENCES jugador(jugador_id),
    CONSTRAINT fk_turno_tipo_turno_id FOREIGN KEY (tipo_turno_id) REFERENCES tipo_turno(tipo_turno_id)
);

DROP INDEX IF EXISTS idx_turno_juego_id;
DROP INDEX IF EXISTS idx_turno_bateador_id;
DROP INDEX IF EXISTS idx_turno_pitcher_id;
DROP INDEX IF EXISTS idx_turno_tipo_turno_id;
CREATE INDEX idx_turno_juego_id ON turno (juego_id);
CREATE INDEX idx_turno_bateador_id ON turno (bateador_id);
CREATE INDEX idx_turno_pitcher_id ON turno (pitcher_id);
CREATE INDEX idx_turno_tipo_turno_id ON turno (tipo_turno_id);

DROP TABLE IF EXISTS lanzamiento CASCADE;
CREATE TABLE IF NOT EXISTS lanzamiento
(
    turno_id      INTEGER NOT NULL ,
    numero_lanzamiento SMALLINT NOT NULL ,
    es_jugada BOOLEAN NOT NULL ,
    es_bola BOOLEAN NOT NULL ,
    es_strike BOOLEAN NOT NULL ,
    es_foul BOOLEAN NOT NULL ,
    es_out BOOLEAN NOT NULL ,
    cuenta_bolas SMALLINT NOT NULL ,
    cuenta_strikes SMALLINT NOT NULL ,
    x REAL,
    y REAL,

    tipo_lanzamiento_id TEXT NOT NULL ,
    
    PRIMARY KEY (turno_id, numero_lanzamiento),
    CONSTRAINT fk_jugada_turno_id FOREIGN KEY (turno_id) REFERENCES turno(turno_id) ON DELETE CASCADE,
    CONSTRAINT fk_jugada_tipo_lanzamiento_id FOREIGN KEY (tipo_lanzamiento_id) REFERENCES tipo_lanzamiento(tipo_lanzamiento_id)
);

DROP INDEX IF EXISTS idx_lanzamiento_tipo_lanzamiento_id;
CREATE INDEX idx_lanzamiento_tipo_lanzamiento_id ON lanzamiento (tipo_lanzamiento_id);