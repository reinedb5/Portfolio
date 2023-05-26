
/* Amount of jobs reviewed over time.
number of jobs reviewed per hour per day for November 2020 */
SELECT ds as dates, 
ROUND((COUNT(job_id)/(SUM(CAST(time_spent AS FLOAT))/3600)), 0) AS Jobs_reviewed
FROM job_data
WHERE ds >= '2020-11-01' AND ds <= '2020-11-30'
GROUP BY ds;


/* Throughput: It is the no. of events happening per second.
7 day rolling average of throughput */
SELECT
ROUND(COUNT(event)/(SUM(TRY_CAST(time_spent AS FLOAT))), 2) AS "weekly_throughput"
FROM job_data;

-- Alternate way
WITH CTE AS (
SELECT ds, COUNT(job_id) AS num_jobs, 
SUM(CAST(time_spent AS float)) AS total_time
FROM job_data
WHERE event IN('transfer','decision')
AND ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds)
SELECT ds, ROUND(1.0*SUM(num_jobs) OVER 
(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)/SUM(total_time) OVER 
(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS throughput_7d
FROM CTE;

-- Daily throughput
SELECT 
  ds AS Dates,
  ROUND(CAST(COUNT(event) AS float),2)/
  ROUND(SUM(CAST(time_spent AS FLOAT)), 2) AS "Daily Throughput"
FROM job_data 
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds 
ORDER BY ds;
-- On date 2020-11-28 the throughput is highest 

/* Percentage share of each language in the last 30 days */

SELECT language AS languages, ROUND(100*COUNT(*)/total, 2) AS percentage 
FROM job_data
CROSS JOIN (SELECT COUNT(*) AS total FROM job_data) AS sub
GROUP BY language, total;

/* Alternate way */

SELECT language AS languages, ROUND(100*COUNT(*)/(SELECT COUNT(*) FROM job_data), 2) AS percentage 
FROM job_data
GROUP BY language;


/* duplicate rows */
SELECT actor_id, COUNT(*) AS duplicates
FROM job_data 
GROUP BY actor_id
HAVING COUNT(*) > 1;

/* weekly user engagement */
SELECT DATEPART(WEEK, occurred_at) AS "Week Numbers", 
COUNT(DISTINCT user_id) AS "Weekly Active Users"
FROM [Operation Analytics].dbo.events
WHERE event_type = 'engagement'
GROUP BY DATEPART(WEEK, occurred_at)
ORDER BY [Week Numbers]; 

/* user growth for product */

SELECT Months, Users, 
    ROUND(((Users/LAG(Users, 1) OVER 
	(ORDER BY Months) -1)*100), 2) AS [Growth in %]
FROM 
( 
    SELECT 
    DATEPART(MONTH, created_at) AS Months, 
    COUNT(activated_at) AS Users
    FROM [Operation Analytics].dbo.users 
    WHERE activated_at IS NOT NULL 
    GROUP BY DATEPART(MONTH, created_at)
) sub;

/* Calculate the weekly retention of users-sign up cohort?  */

SELECT first AS "Week Numbers",
SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "Week 0",
SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "Week 1",
SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "Week 2",
SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "Week 3",
SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "Week 4",
SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "Week 5",
SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "Week 6",
SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "Week 7",
SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "Week 8",
SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "Week 9",
SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "Week 10",
SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "Week 11",
SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "Week 12",
SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "Week 13",
SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "Week 14",
SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "Week 15",
SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "Week 16",
SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "Week 17",
SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "Week 18"
FROM 
( 
SELECT m.user_id, m.login_week, n.first, m.login_week - n.first AS week_number
FROM 
(SELECT user_id, DATEPART(WEEK, occurred_at) AS login_week 
FROM [Operation Analytics].dbo.events
GROUP BY user_id, DATEPART(WEEK, occurred_at)) m
JOIN
(SELECT user_id, MIN(DATEPART(WEEK, occurred_at)) AS first 
FROM [Operation Analytics].dbo.events
GROUP BY user_id) n 
ON m.user_id = n.user_id
) sub 
GROUP BY first 
ORDER BY first;

/* Calculate the weekly engagement per device */

SELECT DATEPART(WEEK, occurred_at) AS "Week Numbers", 
COUNT(DISTINCT CASE WHEN device = 'dell inspiron notebook' THEN user_id END) AS "Dell Inspiron Notebook", 
COUNT(DISTINCT CASE WHEN device = 'iphone 5' THEN user_id END) AS "iPhone 5", 
COUNT(DISTINCT CASE WHEN device = 'iphone 4s' THEN user_id END) AS "iPhone 4S", 
COUNT(DISTINCT CASE WHEN device = 'windows surface' THEN user_id END) AS "Windows Surface",
COUNT(DISTINCT CASE WHEN device = 'macbook air' THEN user_id END) AS "Macbook Air", 
COUNT(DISTINCT CASE WHEN device = 'iphone 5s' THEN user_id END) AS "iPhone 5S", 
COUNT(DISTINCT CASE WHEN device = 'macbook pro' THEN user_id END) AS "Macbook Pro", 
COUNT(DISTINCT CASE WHEN device = 'kindle fire' THEN user_id END) AS "Kindle Fire", 
COUNT(DISTINCT CASE WHEN device = 'ipad mini' THEN user_id END) AS "iPad Mini", 
COUNT(DISTINCT CASE WHEN device = 'nexus 7' THEN user_id END) AS "Nexus 7", 
COUNT(DISTINCT CASE WHEN device = 'nexus 5' THEN user_id END)  AS "Nexus 5",
COUNT(DISTINCT CASE WHEN device = 'samsung galaxy s4' THEN user_id END) AS "Samsung Galaxy S4",
COUNT(DISTINCT CASE WHEN device = 'Lenovo Thinkpad' THEN user_id END) AS "Lenovo Thinkpad",
COUNT(DISTINCT CASE WHEN device = 'Samsumg Galaxy Tablet' THEN user_id END) AS "Samsung Galaxy Tablet",
COUNT(DISTINCT CASE WHEN device = 'Acer Aspire Notebook' THEN user_id END) AS "Acer Aspire Notebook",
COUNT(DISTINCT CASE WHEN device = 'Asus Chromebook' THEN user_id END) AS "Asus Chromebook",
COUNT(DISTINCT CASE WHEN device = 'HTC One' THEN user_id END) AS "HTC One", 
COUNT(DISTINCT CASE WHEN device = 'Nokia Lumia 635' THEN user_id END) AS "Nokia Lumia 635",
COUNT(DISTINCT CASE WHEN device = 'Samsung Galaxy Note' THEN user_id END) AS "Samsung Galaxy Note",
COUNT(DISTINCT CASE WHEN device = 'Acer Aspire Desktop' THEN user_id END) AS "Acer Aspire Desktop",
COUNT(DISTINCT CASE WHEN device = 'Mac Mini' THEN user_id END) AS "Mac Mini", 
COUNT(DISTINCT CASE WHEN device = 'HP Pavilion Desktop' THEN user_id END) AS "HP Pavilion Desktop",
COUNT(DISTINCT CASE WHEN device = 'Dell Inspiron Desktop' THEN user_id END) AS "Dell Inspiron Desktop",
COUNT(DISTINCT CASE WHEN device = 'iPad Air' THEN user_id END) AS "iPad Air", 
COUNT(DISTINCT CASE WHEN device = 'Amazon Fire Phone' THEN user_id END) AS "Amazon Fire Phone",
COUNT(DISTINCT CASE WHEN device = 'Nexus 10' THEN user_id END) AS "Nexus 10"
FROM [Operation Analytics].dbo.events 
WHERE event_type = 'engagement'
GROUP BY DATEPART(WEEK, occurred_at)
ORDER BY DATEPART(WEEK, occurred_at);

/* Calculate the email engagement metrics */

SELECT *
FROM [Operation Analytics].dbo.email_events;

SELECT sub.Week, 
ROUND((CAST(sub.weekly_digest AS float)/CAST(sub.total AS float)*100),2) AS "weekly_digest_rate",
ROUND((CAST(sub.email_opens AS float)/CAST(sub.total AS float)*100),2) AS "Email Open Rate", 
ROUND((CAST(sub.email_clickthroughs AS float)/CAST(sub.total AS float)*100),2) AS "Email Clickthrough Rate", 
ROUND((CAST(sub.reengagement_emails AS float)/CAST(sub.total AS float)*100),2) AS "Reengagement Email Rate" 
FROM 
( 
    SELECT DATEPART(WEEK, occurred_at) AS Week, 
    COUNT(CASE WHEN action = 'sent_weekly_digest' THEN user_id ELSE NULL END) AS weekly_digest,
	COUNT(CASE WHEN action = 'email_open' THEN user_id ELSE NULL END) AS email_opens, 
    COUNT(CASE WHEN action = 'email_clickthrough' THEN user_id ELSE NULL END) AS email_clickthroughs, 
    COUNT(CASE WHEN action = 'sent_reengagement_email' THEN user_id ELSE NULL END) AS reengagement_emails,
    COUNT(user_id) AS total 
    FROM [Operation Analytics].[dbo].[email_events] 
    GROUP BY DATEPART(WEEK, occurred_at)
) sub 
JOIN [Operation Analytics].[dbo].[email_events] AS email_events
ON DATEPART(WEEK, email_events.occurred_at) = sub.Week
GROUP BY sub.Week, sub.weekly_digest, sub.email_opens, sub.email_clickthroughs, sub.reengagement_emails, sub.total
ORDER BY sub.Week;

