SELECT 
	(SELECT COUNT(*) FROM equipo) AS equipo,
	(SELECT COUNT(*) FROM estadio) AS estadio,
	(SELECT COUNT(*) FROM juego) AS juego,
	(SELECT COUNT(*) FROM juego_pitcher) AS juego_pitcher,
	(SELECT COUNT(*) FROM jugador) AS jugador,
	(SELECT COUNT(*) FROM lanzamiento) AS lanzamiento,
    (SELECT COUNT(*) FROM posicion) AS posicion,
    (SELECT COUNT(*) FROM status_juego) AS status_juego,
	(SELECT COUNT(*) FROM tipo_turno) AS tipo_turno,
    (SELECT COUNT(*) FROM tipo_juego) AS tipo_juego,
	(SELECT COUNT(*) FROM turno) AS turno,
	(SELECT COUNT(*) FROM umpire) AS umpire,
	(SELECT MAX(EXTRACT(year FROM primer_lanzamiento)) FROM juego);

SELECT EXTRACT(year FROM primer_lanzamiento) AS año, count(*)
FROM juego
GROUP BY año
ORDER BY año ;