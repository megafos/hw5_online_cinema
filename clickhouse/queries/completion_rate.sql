SELECT toDate(timestamp) AS day, 
       countIf(event_type == 'VIEW_STARTED') AS started_views, 
       countIf(event_type == 'VIEW_FINISHED') AS completed_views,
       round(100.0 * completed_views / NULLIF(started_views, 0), 2) AS completion_rate
FROM cinema.movie_events
GROUP BY day
ORDER BY day DESC;
