-- To check the data type in my Data

SELECT * 
FROM BRIGHT_TV.DATASET.VIEWERSHIP 
LIMIT 10;

SELECT * 
FROM BRIGHT_TV.DATASET.USERPROFILE 
LIMIT 10;


-- Full export-ready query: one row per session with user profile fields,
-- daily/channel aggregates (windowed), CASE classifications, and SA timestamps.
-- Run this in Snowflake and use the client "Export" (CSV/Excel) on the result set.

WITH PARSED_VIEWERSHIP AS (
    SELECT
        USERID,
        CHANNEL2,
        DURATION2,
        -- robust parse (multiple formats) -> SESSION_UTC
        COALESCE(
            TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI'),
            TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY-MM-DD HH24:MI'),
            TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP(RECORDDATE2)
        ) AS SESSION_UTC,
        -- convert to South Africa time -> SESSION_SA
        CONVERT_TIMEZONE(
            'UTC',
            'Africa/Johannesburg',
            COALESCE(
                TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI'),
                TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI:SS'),
                TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY-MM-DD HH24:MI'),
                TRY_TO_TIMESTAMP(RECORDDATE2, 'YYYY-MM-DD HH24:MI:SS'),
                TRY_TO_TIMESTAMP(RECORDDATE2)
            )
        ) AS SESSION_SA
    FROM BRIGHT_TV.DATASET.VIEWERSHIP
)

SELECT
    pv.USERID,
    up.NAME,
    up.SURNAME,
    up.EMAIL,
    up.GENDER,
    up.RACE,
    up.AGE,
    CASE
        WHEN up.AGE IS NULL THEN 'Unknown'
        WHEN up.AGE < 18 THEN 'Youth'
        WHEN up.AGE BETWEEN 18 AND 34 THEN 'Young Adult'
        WHEN up.AGE BETWEEN 35 AND 54 THEN 'Adult'
        ELSE 'Senior'
    END AS AGE_GROUP,
    up.PROVINCE,
    up.SOCIAL_MEDIA_HANDLE,
    CASE
        WHEN up.SOCIAL_MEDIA_HANDLE IS NULL OR TRIM(up.SOCIAL_MEDIA_HANDLE) = '' THEN 'No Handle'
        ELSE 'Has Handle'
    END AS SOCIAL_MEDIA_STATUS,

    pv.CHANNEL2,
    pv.DURATION2,

    pv.SESSION_UTC,
    pv.SESSION_SA,
    DATE(pv.SESSION_SA) AS SESSION_DATE_SA,
    DATE_PART('hour', pv.SESSION_SA) AS HOUR_SA,

    CASE
        WHEN DATE_PART('hour', pv.SESSION_SA) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN DATE_PART('hour', pv.SESSION_SA) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN DATE_PART('hour', pv.SESSION_SA) BETWEEN 17 AND 22 THEN 'Evening'
        ELSE 'Late Night'
    END AS TIME_OF_DAY,

    CASE
        WHEN DAYNAME(DATE(pv.SESSION_SA)) IN ('Saturday','Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS DAY_TYPE,

    -- daily aggregates using window functions (fast, avoids scalar subqueries)
    COUNT(*) OVER (PARTITION BY DATE(pv.SESSION_SA)) AS TOTAL_SESSIONS_PER_DAY,
    COUNT(DISTINCT pv.USERID) OVER (PARTITION BY DATE(pv.SESSION_SA)) AS UNIQUE_USERS_PER_DAY,

    -- per-channel-per-day sessions (windowed)
    COUNT(*) OVER (PARTITION BY DATE(pv.SESSION_SA), pv.CHANNEL2) AS CHANNEL_SESSIONS_PER_DAY,

    -- channel performance tier (based on the channel-day session count)
    CASE
        WHEN COUNT(*) OVER (PARTITION BY DATE(pv.SESSION_SA), pv.CHANNEL2) > 1000 THEN 'High Performing'
        WHEN COUNT(*) OVER (PARTITION BY DATE(pv.SESSION_SA), pv.CHANNEL2) BETWEEN 500 AND 1000 THEN 'Medium Performing'
        ELSE 'Low Performing'
    END AS CHANNEL_PERFORMANCE

FROM PARSED_VIEWERSHIP pv
LEFT JOIN BRIGHT_TV.DATASET.USERPROFILE up
    ON pv.USERID = up.USERID

ORDER BY SESSION_DATE_SA, HOUR_SA, CHANNEL2;
