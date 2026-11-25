SELECT toDate(timestamp) AS day, uniqExact(user_id) AS unique_users 
FROM cinema.movie_events
GROUP BY day
ORDER BY day;
