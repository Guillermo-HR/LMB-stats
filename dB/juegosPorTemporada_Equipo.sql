select equipo_id 
from equipo;

SELECT j.temporada, e.nombre,
       COUNT(CASE WHEN j.local_id = e.equipo_id THEN 1 END) as juegos_local,
       COUNT(CASE WHEN j.visitante_id = e.equipo_id THEN 1 END) as juegos_visitante,
       COUNT(*) as total_partidos_equipo
FROM equipo e
LEFT JOIN juego j ON (e.equipo_id = j.local_id OR e.equipo_id = j.visitante_id) 
GROUP BY j.temporada, e.nombre
ORDER BY j.temporada, e.nombre;