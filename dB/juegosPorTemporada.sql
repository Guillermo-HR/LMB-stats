select temporada, count(*)
from juego
group by temporada
order by temporada;