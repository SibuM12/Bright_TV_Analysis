--- Viewing first 100 rows of the table
SELECT * 
FROM `workspace`.`default`.`user_profile` 
LIMIT 100;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- --

SELECT * 
FROM `workspace`.`default`.`viewership` 
LIMIT 100;

--- Combining the two tables

SELECT 
      u.*,
      v.*
FROM  `workspace`.`default`.`user_profile` AS u
FULL OUTER JOIN `workspace`.`default`.`viewership` AS v
ON u.UserID = v.UserID;


--The conversion of time from UCT to SA time 

SELECT 
    *,
    from_utc_timestamp(TO_TIMESTAMP(RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg') AS sast_timestamp
FROM `workspace`.`default`.`viewership`;


--- Finding Usage by Age Group to show Who drives consumption
SELECT 
    CASE 
        WHEN u.Age < 18 THEN 'Under 18'
        WHEN u.Age BETWEEN 18 AND 25 THEN '18-25'
        WHEN u.Age BETWEEN 26 AND 35 THEN '26-35'
        WHEN u.Age BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+'
    END AS Age_group,
    COUNT(*) AS Total_sessions
FROM `workspace`.`default`.`user_profile` u
JOIN `workspace`.`default`.`viewership` v
    ON u.UserID = v.UserID
GROUP BY 
    CASE 
        WHEN u.Age < 18 THEN 'Under 18'
        WHEN u.Age BETWEEN 18 AND 25 THEN '18-25'
        WHEN u.Age BETWEEN 26 AND 35 THEN '26-35'
        WHEN u.Age BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+'
    END
ORDER BY Total_sessions DESC;


--- Usage by Province to show Regional demand

SELECT 
    u.Province,
    COUNT(*) AS total_sessions
FROM `workspace`.`default`.`user_profile` u
JOIN `workspace`.`default`.`viewership` v
    ON u.UserID = v.UserID
GROUP BY u.Province
ORDER BY total_sessions DESC;



--- Peak Viewing Time (SA Time) to view Peak usage hours
SELECT 
    HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg')) AS Hour_SA,
    COUNT(*) AS Total_sessions
FROM `workspace`.`default`.`viewership` v
JOIN `workspace`.`default`.`user_profile` u
    ON u.UserID = v.UserID
GROUP BY HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg'))
ORDER BY Hour_SA;



--- Usage by Day of Week to view Weekend vs weekday behavior
SELECT Day_name,
       COUNT(*) AS Total_sessions
FROM (
    SELECT 
        date_format(
            from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg'),
            'EEEE'
        ) AS Day_name
    FROM `workspace`.`default`.`viewership` v
    JOIN `workspace`.`default`.`user_profile` u
        ON u.UserID = v.UserID
) sub
GROUP BY Day_name
ORDER BY Total_sessions DESC;

--- Content Preference by Gender
SELECT 
    u.Gender,
    v.Channel2,
    COUNT(*) AS Total_views
FROM `workspace`.`default`.`user_profile` u
JOIN `workspace`.`default`.`viewership` v
    ON u.UserID = v.UserID
GROUP BY u.Gender, v.Channel2
ORDER BY Total_views DESC;


--- Average Session Duration
SELECT 
    v.Channel2,
    AVG(
        (HOUR(v.`Duration 2`) * 3600) +
        (MINUTE(v.`Duration 2`) * 60) +
        SECOND(v.`Duration 2`)
    ) AS Avg_seconds
FROM `workspace`.`default`.`viewership` v
JOIN `workspace`.`default`.`user_profile` u
    ON u.UserID = v.UserID
GROUP BY v.Channel2
ORDER BY Avg_seconds DESC;

--- AGE vs CONTENT
SELECT 
    u.Age,
    v.Channel2,
    COUNT(*) AS Total_views
FROM `workspace`.`default`.`user_profile` u
JOIN `workspace`.`default`.`viewership` v
    ON u.UserID = v.UserID
GROUP BY u.Age, v.Channel2
ORDER BY Total_views DESC;


--- 3. LOW-CONSUMPTION DAYS + CONTENT STRATEGY
---Identifying Lowest Days
SELECT 
    date_format(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),'Africa/Johannesburg'),'EEEE') AS Day_name,
    COUNT(*) AS Total_sessions
FROM `workspace`.`default`.`viewership` v
JOIN `workspace`.`default`.`user_profile` u
    ON u.UserID = v.UserID
GROUP BY Day_name
ORDER BY Total_sessions ASC;


---Best Performing Content
SELECT 
    v.Channel2,
    COUNT(*) AS Total_views
FROM `workspace`.`default`.`viewership` v
JOIN `workspace`.`default`.`user_profile` u
    ON u.UserID = v.UserID
GROUP BY v.Channel2
ORDER BY Total_views DESC
LIMIT 5;

---Weak Content on Low Days (Monday/Tuesday)
SELECT 
    date_format(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
            'Africa/Johannesburg'),'EEEE') AS Day_name,
    v.Channel2,
    COUNT(*) AS Total_views
FROM `workspace`.`default`.`viewership` v
JOIN `workspace`.`default`.`user_profile` u
    ON u.UserID = v.UserID
WHERE date_format(
        from_utc_timestamp(
            to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
            'Africa/Johannesburg'
        ),
        'EEEE'
    ) IN ('Monday','Tuesday')
GROUP BY Day_name, v.Channel2
ORDER BY Total_views ASC;


---THE BIG DATA FOR DASHBAORD
SELECT 
    u.UserID,
    u.Age,
    u.Gender,
    u.Race,
    u.Province,
    v.Channel2,
    to_date(
        from_utc_timestamp(
            to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
            'Africa/Johannesburg'
        )
    ) AS record_date,
    date_format(
        from_utc_timestamp(
            to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
            'Africa/Johannesburg'
        ),
        'HH:mm:ss'
    ) AS record_time,
    HOUR(
        from_utc_timestamp(
            to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
            'Africa/Johannesburg'
        )
    ) AS Hour_SA,
    date_format(
        from_utc_timestamp(
            to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
            'Africa/Johannesburg'
        ),
        'EEEE'
    ) AS Day_Name,
    CASE
        WHEN HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg')) BETWEEN 6 AND 9 THEN 'Early Morning'
        WHEN HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg')) BETWEEN 10 AND 12 THEN 'Late Morning'
        WHEN HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg')) BETWEEN 13 AND 15 THEN 'Early Afternoon'
        WHEN HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg')) BETWEEN 16 AND 18 THEN 'Late Afternoon'
        WHEN HOUR(from_utc_timestamp(to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'), 'Africa/Johannesburg')) BETWEEN 19 AND 22 THEN 'Evening'
        ELSE 'Late Night'
    END AS Time_bucket,
    date_format(
        to_timestamp(v.RecordDate2, 'yyyy/MM/dd HH:mm'),
        'MMMM'
    ) AS Month_name,
    (
        (HOUR(v.`Duration 2`) * 3600) +
        (MINUTE(v.`Duration 2`) * 60) +
        SECOND(v.`Duration 2`)
    ) AS Duration_Seconds,
    CASE
        WHEN u.Age < 18 THEN 'Under 18'
        WHEN u.Age BETWEEN 18 AND 25 THEN '18-25'
        WHEN u.Age BETWEEN 26 AND 35 THEN '26-35'
        WHEN u.Age BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+'
    END AS Age_group
FROM `workspace`.`default`.`user_profile` u
JOIN `workspace`.`default`.`viewership` v
    ON u.UserID = v.UserID;
