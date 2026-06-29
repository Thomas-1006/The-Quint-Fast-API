WITH all_events AS (

  -- Historical data (ALL past data)
  SELECT *
  FROM `growthnow.analytics_519922935.events_*`
  WHERE _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

  UNION ALL

  -- Today's intraday data
  SELECT *
  FROM `growthnow.analytics_519922935.events_intraday_*`
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
),

base AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)
         + INTERVAL 5 HOUR + INTERVAL 30 MINUTE) AS event_date_ist,

    user_pseudo_id,

    CONCAT(
      user_pseudo_id, '-',
      CAST(
        (SELECT value.int_value
         FROM UNNEST(event_params)
         WHERE key = 'ga_session_id') AS STRING
      )
    ) AS session_id,

    event_name,

    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'page_title') AS page_title,

    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'engagement_time_msec') AS engagement_time_msec,

    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'source') AS source,

    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'medium') AS medium,

    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'campaign') AS campaign,

    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'content') AS content

  FROM all_events
)

SELECT
  user_pseudo_id AS user_id,
  COALESCE(source, '(direct)') AS source,
  COUNTIF(event_name = 'page_view') AS page_views,
  COUNT(DISTINCT session_id) AS sessions,
  ROUND(SUM(engagement_time_msec) / 1000, 2) AS total_time_spent_sec

FROM base
WHERE page_title IS NOT NULL

GROUP BY
  user_id,
  source

HAVING COUNTIF(event_name = 'page_view') > 3

ORDER BY
  page_views DESC

LIMIT 10;