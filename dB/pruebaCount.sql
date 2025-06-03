SELECT 
	(SELECT COUNT(*) FROM equipo) AS equipo,
	(SELECT COUNT(*) FROM estadio) AS estadio,
	(SELECT COUNT(*) FROM juego) AS juego,
	(SELECT COUNT(*)/1000 FROM juego_pitcher) AS juego_pitcher,
	(SELECT COUNT(*)/1000 FROM juego_bateador) AS juego_bateador,
	(SELECT COUNT(*) FROM jugador) AS jugador,
	(SELECT COUNT(*)/1000 FROM lanzamiento) AS lanzamiento,
    (SELECT COUNT(*) FROM posicion) AS posicion,
    (SELECT COUNT(*) FROM status_juego) AS status_juego,
	(SELECT COUNT(*) FROM tipo_turno) AS tipo_turno,
    (SELECT COUNT(*) FROM tipo_juego) AS tipo_juego,
	(SELECT COUNT(*)/1000 FROM turno) AS turno,
	(SELECT COUNT(*) FROM umpire) AS umpire;

SELECT EXTRACT(year FROM primer_lanzamiento) AS año, count(*)
FROM juego
GROUP BY año
ORDER BY año ;