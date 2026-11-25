WITH first_visits AS (
        SELECT user_id, min(toDate(timestamp)) AS first_day FROM cinema.movie_events 
        WHERE event_type = 'VIEW_STARTED' GROUP BY user_id
    ),
    activity_days AS (
        SELECT DISTINCT user_id, toDate(timestamp) AS activity_day FROM cinema.movie_events
    ),
    user_retention AS (
        SELECT fv.user_id, fv.first_day, ad.activity_day, dateDiff('day', fv.first_day, ad.activity_day) AS days_since_first 
        FROM first_visits AS fv 
        JOIN activity_days AS ad ON fv.user_id = ad.user_id 
    )
SELECT first_day, 
       countIf(days_since_first = 0) AS cohort_size, 
       countIf(days_since_first = 1) AS retained_d1, 
       countIf(days_since_first = 7) AS retained_d7,
       if (dateDiff('day', first_day, today()) > 1, round(100.0 * retained_d1 / NULLIF(cohort_size, 0), 2), NULL) AS retention_d1,
       if (dateDiff('day', first_day, today()) > 7, round(100.0 * retained_d7 / NULLIF(cohort_size, 0), 2), NULL) AS retention_d7
FROM user_retention
GROUP BY first_day
ORDER BY first_day;
