SELECT movie_id, COUNT(*) AS started_views
FROM cinema.movie_events
WHERE event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY started_views DESC
LIMIT 5;
