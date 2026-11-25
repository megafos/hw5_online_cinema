SELECT toDate(timestamp) AS day, round(avg(progress_seconds)) AS avg_watch_seconds
FROM cinema.movie_events
WHERE event_type = 'VIEW_FINISHED'
GROUP BY day
ORDER BY day;
