-- Returns distance in miles between two points (lat1, long1, lat1, long1)
CREATE OR REPLACE FUNCTION compute_distance_between_points(FLOAT, FLOAT, FLOAT, FLOAT)
RETURNS FLOAT
IMMUTABLE
AS $$
  SELECT
    2 * 3961 * ASIN( SQRT( ( SIN( RADIANS(($3 - $1) / 2) ) ) ^ 2 + COS(RADIANS($1)) * COS(RADIANS($3)) * (SIN(RADIANS(($4 - $2) / 2))) ^ 2))
$$ LANGUAGE sql
;

-- Given ds, returns event_ds, which is ds + lead_time.
CREATE OR REPLACE FUNCTION {{ env.MAILING_NAME }}_compute_event_ds(DATE)
RETURNS DATE
IMMUTABLE
AS $$
  SELECT DATEADD(day, {{ env.INVITE_LEAD_DAYS }}, $1)::DATE
$$ LANGUAGE sql
;

-- Given dayofweek of event_ds, returns window length in days.
CREATE OR REPLACE FUNCTION {{ env.MAILING_NAME }}_compute_event_window_length(INTEGER)
RETURNS INTEGER
IMMUTABLE
AS $$
  SELECT CASE WHEN $1 = 0 THEN 3  -- Sunday is Sun-Tues
              WHEN $1 = 1 THEN 3  -- Monday is Mon-Weds
              WHEN $1 = 2 THEN 2  -- Tuesday is Tues-Weds
              WHEN $1 = 3 THEN 5  -- Wednesday is Weds-Sun
              WHEN $1 = 4 THEN 4  -- Thursday is Thurs-Sun
              WHEN $1 = 5 THEN 3  -- Friday is Fri-Sun
              WHEN $1 = 6 THEN 3  -- Saturday is Sat-Mon
         END
$$ LANGUAGE sql
;

-- Given ds, returns date after last day of event window, which is
-- ds + lead_time + event_window_length.
CREATE OR REPLACE FUNCTION {{ env.MAILING_NAME }}_compute_event_window_end_ds(DATE)
RETURNS DATE
IMMUTABLE
AS $$
  SELECT DATEADD(day, {{ env.MAILING_NAME }}_compute_event_window_length(EXTRACT(DAYOFWEEK FROM {{ env.MAILING_NAME }}_compute_event_ds($1))), {{ env.MAILING_NAME }}_compute_event_ds($1))::DATE
$$ LANGUAGE sql
;

-- Given ds, returns first full day of quiet period.
-- This is (ds - (quiet_period_days - 1)).
CREATE OR REPLACE FUNCTION {{ env.MAILING_NAME }}_compute_quiet_period_start_ds(DATE)
RETURNS DATE
IMMUTABLE
AS $$
  SELECT DATEADD(day, -({{ env.QUIET_PERIOD_DAYS }} - 1), $1)::DATE
$$ LANGUAGE sql
;

DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_cons_for_event_build;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_cons_for_event_build AS
SELECT
  {{ env.DS | pprint }}::DATE AS ds
, cons_email.email
, cons.cons_id
, cons.firstname
, cons.lastname
, cons_addr.addr1
, cons_addr.city
, cons_addr.state_cd
, state_abbr.state AS state_name
, cons_addr.zip
, CASE WHEN cons_addr.latitude IS NULL OR cons_addr.latitude = 0 THEN zips.latitude
       ELSE cons_addr.latitude END AS latitude
, CASE WHEN cons_addr.longitude IS NULL OR cons_addr.longitude = 0 THEN zips.longitude
       ELSE cons_addr.longitude END AS longitude
  FROM bsd.cons_email_chapter_subscription
  LEFT OUTER JOIN bsd.cons_group_27db4a4ef751 AS inactive_constituents_hotmail_live_msn
    ON inactive_constituents_hotmail_live_msn.cons_id = cons_email_chapter_subscription.cons_id
  LEFT OUTER JOIN bsd.cons_group_62d08f7e9f49 AS inactive_constituents
    ON inactive_constituents.cons_id = cons_email_chapter_subscription.cons_id
  LEFT OUTER JOIN bsd.cons_group_72dc10463d3b AS returnpath_seed_list
    ON returnpath_seed_list.cons_id = cons_email_chapter_subscription.cons_id
  JOIN bsd.cons_email
    ON cons_email.cons_email_id = cons_email_chapter_subscription.cons_email_id
   AND cons_email.is_primary
  JOIN bsd.cons
    ON cons.cons_id = cons_email_chapter_subscription.cons_id
  JOIN bsd.cons_addr
    ON cons_addr.cons_id = cons_email_chapter_subscription.cons_id
   AND cons_addr.is_primary = 1
  LEFT OUTER JOIN extern.zips
    ON zips.zip = cons_addr.zip
  LEFT OUTER JOIN extern.state_abbr
    ON state_abbr.state_abbr = cons_addr.state_cd
 WHERE cons_email_chapter_subscription.isunsub = 0
   AND cons_email_chapter_subscription.chapter_id = 1
   AND inactive_constituents_hotmail_live_msn.cons_id IS NULL
   AND inactive_constituents.cons_id IS NULL
   AND returnpath_seed_list.cons_id IS NULL
   AND (cons_addr.latitude IS NOT NULL OR zips.latitude IS NOT NULL)
   AND (cons_addr.longitude IS NOT NULL OR zips.longitude IS NOT NULL)
   {% if 'STATE_WHITELIST' in env and env.STATE_WHITELIST %}
     AND cons_addr.state_cd IN ({{ env.STATE_WHITELIST.split(',') | map('pprint') | join(', ') }})
   {% endif %}
   {% if 'STATE_BLACKLIST' in env and env.STATE_BLACKLIST %}
     AND cons_addr.state_cd NOT IN ({{ env.STATE_BLACKLIST.split(',') | map('pprint') | join(', ') }})
   {% endif %}
;

-- Event ID and next timeslot on or after event_ds
DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_next_timeslot_event_invite;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_next_timeslot_event_invite AS
SELECT
  t.ds
, t.event_ds
, t.next_timeslot_id
, t.event_id
, t.start_date
, t.start_timestamp
, t.max_attendees
, rsvp_count_per_timeslot_id.rsvp_count
, t.max_attendees - COALESCE(rsvp_count_per_timeslot_id.rsvp_count, 0) AS spots_remaining
, t.formatted_start_times
, t.csv_timeslot_urls
, t.csv_start_times
  FROM (
    SELECT DISTINCT *
      FROM (
        WITH timeslots_w_daily_formatted_start_times AS (
          SELECT
              timeslots.id
            , timeslots.event_id
            , timeslots.start_date
            , timeslots.max_attendees
            , timeslots.deleted_date

            -- URL per timeslot of day, separated by '|'
            , LISTAGG('https://events.elizabethwarren.com/event/' || timeslots.event_id || '/?timeslot=' || timeslots.id, '|') WITHIN GROUP (ORDER BY timeslots.start_date ASC) OVER(PARTITION BY events.id, convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP)::DATE) AS daily_csv_timeslot_urls

            -- Human-formatted start time per timeslot of day, separated by '|'
            , LISTAGG(TRIM(TO_CHAR(timeslots.start_date AT TIME ZONE events.timezone, 'Day')) || ' at ' || LTRIM(TO_CHAR(timeslots.start_date AT TIME ZONE events.timezone, 'HH:MIam'), '0'), '|') WITHIN GROUP (ORDER BY timeslots.start_date ASC) OVER(PARTITION BY events.id, convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP)::DATE) AS daily_csv_start_times

            -- Human-formatted synopsis of times available on this day
            -- We don't include date ("February 22") for clarity when putting on a CTA button.
            , TRIM(TO_CHAR(timeslots.start_date AT TIME ZONE events.timezone, 'Day')) || ', ' || TRIM(TO_CHAR(timeslots.start_date AT TIME ZONE events.timezone, 'Month')) || ' ' || TRIM(TO_CHAR(timeslots.start_date AT TIME ZONE events.timezone, 'DD')) ||
                CASE WHEN COUNT(timeslots.start_date) OVER(PARTITION BY events.id, convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP)::DATE) >= 3 THEN
                  '' -- Could be 'throughout the day' or 'at multiple times' but this seems superfluous
                ELSE ' at ' || LISTAGG(LTRIM(TO_CHAR(timeslots.start_date AT TIME ZONE events.timezone, 'HH:MIam'), '0'), ' and ') WITHIN GROUP (ORDER BY timeslots.start_date ASC) OVER(PARTITION BY events.id, convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP)::DATE)
                END
                AS daily_formatted_start_times
            FROM mobilizeamerica.events
            JOIN mobilizeamerica.timeslots
              ON timeslots.event_id = events.id
             AND convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP) >= {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE)
             AND timeslots.deleted_date IS NULL
        )
        SELECT
          {{ env.DS | pprint }}::DATE AS ds
        , {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE) AS event_ds
        , events.id AS event_id
        , convert_timezone('America/New_York', timeslots_w_daily_formatted_start_times.start_date::TIMESTAMP)::DATE AS start_date
        , timeslots_w_daily_formatted_start_times.start_date AS start_timestamp
        , timeslots_w_daily_formatted_start_times.max_attendees
        , timeslots_w_daily_formatted_start_times.id AS next_timeslot_id

        -- Human-formatted synopsis of all start times of event
        , LISTAGG(DISTINCT timeslots_w_daily_formatted_start_times.daily_formatted_start_times, ' and ') WITHIN GROUP (ORDER BY timeslots_w_daily_formatted_start_times.start_date ASC) OVER(PARTITION BY events.id) AS formatted_start_times

        -- All URLs of timeslots of event, separated by '|'
        , LISTAGG(DISTINCT timeslots_w_daily_formatted_start_times.daily_csv_timeslot_urls, '|') WITHIN GROUP (ORDER BY timeslots_w_daily_formatted_start_times.start_date ASC) OVER(PARTITION BY events.id) AS csv_timeslot_urls

        -- All human-formatted start times of event, separated by '|'
        , LISTAGG(DISTINCT timeslots_w_daily_formatted_start_times.daily_csv_start_times, '|') WITHIN GROUP (ORDER BY timeslots_w_daily_formatted_start_times.start_date ASC) OVER(PARTITION BY events.id) AS csv_start_times
        , ROW_NUMBER() OVER (PARTITION BY events.id ORDER BY timeslots_w_daily_formatted_start_times.start_date ASC) AS start_date_rank
          FROM mobilizeamerica.events
          JOIN timeslots_w_daily_formatted_start_times
            ON events.id = timeslots_w_daily_formatted_start_times.event_id
           AND convert_timezone('America/New_York', timeslots_w_daily_formatted_start_times.start_date::TIMESTAMP) >= {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE)
           -- Showcase only timeslots on event_ds and the next three days (to catch all GOTV events).
           AND convert_timezone('America/New_York', timeslots_w_daily_formatted_start_times.start_date::TIMESTAMP) < DATEADD(day, 4, {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE))::DATE
           AND timeslots_w_daily_formatted_start_times.deleted_date IS NULL
    )
    WHERE start_date_rank = 1
) t
  LEFT OUTER JOIN (
  SELECT
    participations.timeslot_id
  , COUNT(*) as rsvp_count
    FROM mobilizeamerica.participations
   WHERE participations.status != 'CANCELLED'
   GROUP BY participations.timeslot_id
) rsvp_count_per_timeslot_id
    ON rsvp_count_per_timeslot_id.timeslot_id = t.next_timeslot_id
;

-- Events with all details in the date range event_ds, event_window_end
DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_for_build_features;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_for_build_features AS
SELECT
  {{ env.DS | pprint }}::DATE AS ds
, {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE) AS event_ds
, events.id AS event_id
, 'https://events.elizabethwarren.com/event/' || events.id || '/' {% if env.MAILING_NAME == 'event_invite' %}|| '?utm_source=email-{{ env.DS | replace("-", "") }}-{{ env.MAILING_NAME }}'{% endif %} AS event_url
, events.title AS event_title
, events.summary AS event_summary
, events.event_type
, events.location__venue AS event_venue
, CASE WHEN events.location__is_private = 1 THEN ''
       ELSE events.location__address_line_1
   END AS event_addr1
, events.location__locality AS event_city
, COALESCE(events.location__region, events.organization__state) AS event_state
, events.location__postal_code AS event_zip
, COALESCE(events.location__lat, zips.latitude) AS latitude
, COALESCE(events.location__lon, zips.longitude) AS longitude
, events.timezone AS event_timezone
, {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_date AS event_start_date
, TO_CHAR({{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_timestamp AT TIME ZONE events.timezone, 'YYYY-MM-DD HH24:MI:SS') AS event_start_timestamp_local
, TO_CHAR({{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_timestamp AT TIME ZONE events.timezone, 'Day, Month DD, YYYY') AS event_start_formatted_date_local
, TO_CHAR({{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_timestamp AT TIME ZONE events.timezone, 'HH:MI am') AS event_start_formatted_time_local
, TO_CHAR({{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_timestamp AT TIME ZONE events.timezone, 'Day') AS event_start_day_of_week_local
, EXTRACT(DAYOFWEEK FROM {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_date) AS dayofweek
, {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.spots_remaining AS event_spots_remaining
, {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.formatted_start_times AS event_formatted_start_times
, {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.csv_timeslot_urls AS event_csv_timeslot_urls
, {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.csv_start_times AS event_csv_start_times
  FROM mobilizeamerica.events
  JOIN {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_next_timeslot_event_invite
    ON {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.ds = {{ env.DS | pprint }}::DATE
   AND {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.event_id = events.id
  LEFT OUTER JOIN extern.zips
    ON zips.zip = events.location__postal_code
  {% if 'TAG_ID_WHITELIST' in env and env.TAG_ID_WHITELIST %}
  JOIN mobilizeamerica.event_tags
    ON event_tags.event_id = events.id
   AND event_tags.tag_id IN ({{ env.TAG_ID_WHITELIST }})
  {% endif %}
 WHERE events.visibility = 'PUBLIC'
   AND events.approval_status = 'APPROVED'
   AND events.deleted_date IS NULL
   {% if 'EVENT_TYPE_WHITELIST' in env and env.EVENT_TYPE_WHITELIST %}
     AND events.event_type IN ({{ env.EVENT_TYPE_WHITELIST.split(',') | map('pprint') | join(', ') }})
   {% else %}
     AND events.event_type IN ('CANVASS')
   {% endif %}
-- Include events with no city.
--   AND events.location__locality IS NOT NULL
--   AND events.location__locality != ''
   AND events.is_virtual != 1
   AND {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_date >= {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE)
   AND {{ env.MAILING_NAME }}_events_next_timeslot_event_invite.start_date < {{ env.MAILING_NAME }}_compute_event_window_end_ds({{ env.DS | pprint }}::DATE)
  {% if 'STATE_WHITELIST' in env and env.STATE_WHITELIST %}
    AND COALESCE(events.location__region, events.organization__state) IN ({{ env.STATE_WHITELIST.split(',') | map('pprint') | join(', ') }})
  {% endif %}
  {% if 'STATE_BLACKLIST' in env and env.STATE_BLACKLIST %}
    AND COALESCE(events.location__region, events.organization__state) NOT IN ({{ env.STATE_BLACKLIST.split(',') | map('pprint') | join(', ') }})
  {% endif %}
;

-- Scored events
DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_for_build;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_for_build AS
SELECT
  {{ env.MAILING_NAME }}_events_for_build_features.*
, CASE WHEN {{ env.MAILING_NAME }}_events_for_build_features.dayofweek IN (0, 6) THEN 2 -- Prefer weekend over weekday
       ELSE 1 END
  *
  -- We don't weight canvasses specially because we tend to send two
  -- kinds of mails:
  --   a) Canvss only
  --   b) Non-Canvass only
  CASE WHEN {{ env.MAILING_NAME }}_events_for_build_features.event_type IN ('PHONE_BANK') THEN 256  -- Always prefer phone bank if available
       WHEN {{ env.MAILING_NAME }}_events_for_build_features.event_type IN ('BARNSTORM', 'TRAINING') THEN 16 -- Always prefer barnstorm, training if no phone bank
       ELSE 1 END
    AS score_event
  FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_for_build_features
 WHERE {{ env.MAILING_NAME }}_events_for_build_features.ds = {{ env.DS | pprint }}::DATE
;

-- For every cons, all events within 25 miles.
-- This first pass does not have filter around max attendees.
DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features_first_pass;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features_first_pass AS
SELECT
  {{ env.DS | pprint }}::DATE AS ds
, {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE) AS event_ds
, {{ env.MAILING_NAME }}_cons_for_event_build.email
, {{ env.MAILING_NAME }}_cons_for_event_build.cons_id
, {{ env.MAILING_NAME }}_cons_for_event_build.firstname
, {{ env.MAILING_NAME }}_cons_for_event_build.lastname
, {{ env.MAILING_NAME }}_cons_for_event_build.addr1 AS cons_addr1
, {{ env.MAILING_NAME }}_cons_for_event_build.city AS cons_city
, {{ env.MAILING_NAME }}_cons_for_event_build.state_cd AS cons_state
, {{ env.MAILING_NAME }}_cons_for_event_build.state_name AS cons_state_name
, {{ env.MAILING_NAME }}_cons_for_event_build.zip AS cons_zip
, {{ env.MAILING_NAME }}_cons_for_event_build.latitude AS cons_latitude
, {{ env.MAILING_NAME }}_cons_for_event_build.longitude AS cons_longitude
, compute_distance_between_points({{ env.MAILING_NAME }}_cons_for_event_build.latitude, {{ env.MAILING_NAME }}_cons_for_event_build.longitude, {{ env.MAILING_NAME }}_events_for_build.latitude, {{ env.MAILING_NAME }}_events_for_build.longitude) AS distance_to_event
, {{ env.MAILING_NAME }}_events_for_build.score_event
, {{ env.MAILING_NAME }}_events_for_build.event_id
, {{ env.MAILING_NAME }}_events_for_build.event_url
, {{ env.MAILING_NAME }}_events_for_build.event_title
, {{ env.MAILING_NAME }}_events_for_build.event_summary
, {{ env.MAILING_NAME }}_events_for_build.event_type
, {{ env.MAILING_NAME }}_events_for_build.event_venue
, {{ env.MAILING_NAME }}_events_for_build.event_addr1
, {{ env.MAILING_NAME }}_events_for_build.event_city
, {{ env.MAILING_NAME }}_events_for_build.event_state
, {{ env.MAILING_NAME }}_events_for_build.event_zip
, {{ env.MAILING_NAME }}_events_for_build.latitude AS event_latitude
, {{ env.MAILING_NAME }}_events_for_build.longitude AS event_longitude
, {{ env.MAILING_NAME }}_events_for_build.event_timezone
, {{ env.MAILING_NAME }}_events_for_build.event_start_date
, {{ env.MAILING_NAME }}_events_for_build.event_start_timestamp_local
, {{ env.MAILING_NAME }}_events_for_build.event_start_formatted_date_local
, {{ env.MAILING_NAME }}_events_for_build.event_start_formatted_time_local
, {{ env.MAILING_NAME }}_events_for_build.event_start_day_of_week_local
, {{ env.MAILING_NAME }}_events_for_build.event_spots_remaining
, {{ env.MAILING_NAME }}_events_for_build.event_formatted_start_times
, {{ env.MAILING_NAME }}_events_for_build.event_csv_timeslot_urls
, {{ env.MAILING_NAME }}_events_for_build.event_csv_start_times
  FROM (
    SELECT * FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_cons_for_event_build
    WHERE ds={{ env.DS | pprint }}::DATE
  ) {{ env.MAILING_NAME }}_cons_for_event_build
 CROSS JOIN (
    SELECT * FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_for_build
    WHERE ds={{ env.DS | pprint }}::DATE
 ) {{ env.MAILING_NAME }}_events_for_build
WHERE {{ env.MAILING_NAME }}_events_for_build.latitude IS NOT NULL
  AND {{ env.MAILING_NAME }}_events_for_build.longitude IS NOT NULL
  AND {{ env.MAILING_NAME }}_cons_for_event_build.latitude IS NOT NULL
  AND {{ env.MAILING_NAME }}_cons_for_event_build.longitude IS NOT NULL
  AND distance_to_event < {% if 'RADIUS' in env and env.RADIUS %}{{ env.RADIUS }}{% else %}25{% endif %}
  AND {{ env.MAILING_NAME }}_events_for_build.event_state = {{ env.MAILING_NAME }}_cons_for_event_build.state_cd
;

-- For every cons, events with capacity within 25 miles.
-- This second pass limits to events which have fewer than 5000 expected email
-- recipients per remaining spot.
DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features AS
SELECT
  {{ env.MAILING_NAME }}_events_per_cons_features_first_pass.*
, nearby_cons_count_per_event.cons_count
, CASE WHEN {{ env.MAILING_NAME }}_events_per_cons_features_first_pass.event_spots_remaining = 0 THEN 1000000
       ELSE nearby_cons_count_per_event.cons_count / {{ env.MAILING_NAME }}_events_per_cons_features_first_pass.event_spots_remaining END AS recipients_per_available_spots
  FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features_first_pass
  JOIN (
  SELECT
    event_id
  , COUNT(*) AS cons_count
    FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features_first_pass
   WHERE ds = {{ env.DS | pprint }}::DATE
   GROUP BY event_id
) nearby_cons_count_per_event
    ON nearby_cons_count_per_event.event_id = {{ env.MAILING_NAME }}_events_per_cons_features_first_pass.event_id
 WHERE
  recipients_per_available_spots IS NULL OR recipients_per_available_spots < {{ env.MAX_RECIPIENTS_PER_SPOTS_REMAINING }}
;

-- Scored events per cons
DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons AS
SELECT
  {{ env.MAILING_NAME }}_events_per_cons_features.*
, CASE WHEN {{ env.MAILING_NAME }}_events_per_cons_features.distance_to_event <= 7 THEN 4  -- Prefer less-than-seven-miles-away events to farther
       ELSE 1 END * {{ env.MAILING_NAME }}_events_per_cons_features.score_event - {{ env.MAILING_NAME }}_events_per_cons_features.distance_to_event * 0.001 -- All else same, prefer closer
    AS score_event_cons
  FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons_features
 WHERE ds = {{ env.DS | pprint }}::DATE
;

DROP TABLE IF EXISTS {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_event_per_cons;
CREATE TABLE {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_event_per_cons AS
SELECT
  {{ env.DS | pprint }}::DATE AS ds
, {{ env.MAILING_NAME }}_compute_event_ds({{ env.DS | pprint }}::DATE) AS event_ds
, {{ env.MAILING_NAME }}_events_per_cons.cons_id
, {{ env.MAILING_NAME }}_events_per_cons.email
, NULL::BIGINT AS secondary_id
, {{ env.MAILING_NAME | pprint }} AS mailing_name
, {{ env.MAILING_NAME }}_events_per_cons.firstname
, {{ env.MAILING_NAME }}_events_per_cons.lastname
, {{ env.MAILING_NAME }}_events_per_cons.cons_addr1
, {{ env.MAILING_NAME }}_events_per_cons.cons_city
, {{ env.MAILING_NAME }}_events_per_cons.cons_state
, {{ env.MAILING_NAME }}_events_per_cons.cons_state_name
, {{ env.MAILING_NAME }}_events_per_cons.cons_zip
, {{ env.MAILING_NAME }}_events_per_cons.cons_latitude
, {{ env.MAILING_NAME }}_events_per_cons.cons_longitude
, {{ env.MAILING_NAME }}_events_per_cons.distance_to_event
, {{ env.MAILING_NAME }}_events_per_cons.score_event
, {{ env.MAILING_NAME }}_events_per_cons.score_event_cons
, {{ env.MAILING_NAME }}_events_per_cons.event_id
, {{ env.MAILING_NAME }}_events_per_cons.event_url
, {{ env.MAILING_NAME }}_events_per_cons.event_title
, {{ env.MAILING_NAME }}_events_per_cons.event_summary
, {{ env.MAILING_NAME }}_events_per_cons.event_type
, {{ env.MAILING_NAME }}_events_per_cons.event_venue
, {{ env.MAILING_NAME }}_events_per_cons.event_addr1
, {{ env.MAILING_NAME }}_events_per_cons.event_city
, {{ env.MAILING_NAME }}_events_per_cons.event_state
, {{ env.MAILING_NAME }}_events_per_cons.event_zip
, {{ env.MAILING_NAME }}_events_per_cons.event_latitude
, {{ env.MAILING_NAME }}_events_per_cons.event_longitude
, {{ env.MAILING_NAME }}_events_per_cons.event_timezone
, {{ env.MAILING_NAME }}_events_per_cons.event_start_date
, {{ env.MAILING_NAME }}_events_per_cons.event_start_timestamp_local
, {{ env.MAILING_NAME }}_events_per_cons.event_start_formatted_date_local
, {{ env.MAILING_NAME }}_events_per_cons.event_start_formatted_time_local
, {{ env.MAILING_NAME }}_events_per_cons.event_start_day_of_week_local
, {{ env.MAILING_NAME }}_events_per_cons.event_formatted_start_times
, {{ env.MAILING_NAME }}_events_per_cons.event_csv_timeslot_urls
, {{ env.MAILING_NAME }}_events_per_cons.event_csv_start_times
  FROM (
    SELECT
      *
    , ROW_NUMBER() OVER (PARTITION BY {{ env.MAILING_NAME }}_events_per_cons.cons_id ORDER BY {{ env.MAILING_NAME }}_events_per_cons.score_event_cons DESC) AS score_rank
      FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_events_per_cons
     WHERE {{ env.MAILING_NAME }}_events_per_cons.ds = {{ env.DS | pprint }}::DATE
  ) {{ env.MAILING_NAME }}_events_per_cons
 WHERE {{ env.MAILING_NAME }}_events_per_cons.score_rank = 1
;

-- If there are no sends for this ds, delete all rows with this ds from
-- schedulings.
DELETE FROM {{ env.SCHEMA }}.triggered_email_schedulings
 WHERE triggered_email_schedulings.ds = {{ env.DS | pprint }}::DATE
   AND triggered_email_schedulings.mailing_name={{ env.MAILING_NAME | pprint }}
   AND (SELECT
          COUNT(*)
          FROM {{ env.SCHEMA }}.triggered_email_sends
         WHERE triggered_email_sends.ds = {{ env.DS | pprint }}::DATE
           AND triggered_email_sends.mailing_name={{ env.MAILING_NAME | pprint }}
       ) = 0;

-- Don't send during quiet period after the last event invite to this email.
DROP TABLE IF EXISTS {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging;
CREATE TEMP TABLE {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging AS
SELECT
  {{ env.MAILING_NAME }}_event_per_cons.*
  FROM {{ env.SCHEMA }}.{{ env.MAILING_NAME }}_event_per_cons

  -- Don't send during quiet period after the last event invite to this email.
  LEFT OUTER JOIN {{ env.SCHEMA }}.triggered_email_schedulings
    ON triggered_email_schedulings.ds >= {{ env.MAILING_NAME }}_compute_quiet_period_start_ds({{ env.DS | pprint }}::DATE)
   AND triggered_email_schedulings.ds <= {{ env.MAILING_NAME }}_event_per_cons.ds
   AND triggered_email_schedulings.email = {{ env.MAILING_NAME }}_event_per_cons.email
   AND ((triggered_email_schedulings.secondary_id IS NULL AND {{ env.MAILING_NAME }}_event_per_cons.secondary_id IS NULL) OR triggered_email_schedulings.secondary_id = {{ env.MAILING_NAME }}_event_per_cons.secondary_id)
   AND triggered_email_schedulings.mailing_name = {{ env.MAILING_NAME }}_event_per_cons.mailing_name

 WHERE {{ env.MAILING_NAME }}_event_per_cons.ds = {{ env.DS | pprint }}::DATE
   AND {{ env.MAILING_NAME }}_event_per_cons.event_start_date = {{ env.MAILING_NAME }}_event_per_cons.event_ds
   AND triggered_email_schedulings.email IS NULL
;

-- Populate triggered_email_payloads_event_invite with all columns.
-- We populate payloads before schedulings to avoid situation where
-- there is a schedulings row but not a payloads row.
BEGIN TRANSACTION;
  DELETE FROM {{ env.SCHEMA }}.triggered_email_payloads_{{ env.MAILING_NAME }}
   USING {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging
   WHERE triggered_email_payloads_{{ env.MAILING_NAME }}.ds = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.ds
     AND triggered_email_payloads_{{ env.MAILING_NAME }}.email = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.email
     AND ((triggered_email_payloads_{{ env.MAILING_NAME }}.secondary_id IS NULL AND {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.secondary_id IS NULL) OR triggered_email_payloads_{{ env.MAILING_NAME }}.secondary_id = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.secondary_id)
     AND triggered_email_payloads_{{ env.MAILING_NAME }}.mailing_name = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.mailing_name
  ;

INSERT INTO {{ env.SCHEMA }}.triggered_email_payloads_{{ env.MAILING_NAME }}
  SELECT
    ds
  , cons_id
  , email
  , secondary_id
  , mailing_name
  , cons_city
  , cons_state
  , cons_zip
  , distance_to_event
  , event_url
  , event_title
  , event_summary
  , event_type
  , event_venue
  , event_addr1
  , event_city
  , event_state
  , event_zip
  , event_timezone
  , event_start_timestamp_local
  , GETDATE() AS payload_prepared_at
  , cons_state_name
  , firstname
  , lastname
  , event_start_formatted_date_local
  , event_start_formatted_time_local
  , event_start_day_of_week_local
  , event_formatted_start_times
  , event_csv_timeslot_urls
  , event_csv_start_times
    FROM {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging
  ;
END TRANSACTION;

-- Populate triggered_email_schedulings with the first 5 columns.
BEGIN TRANSACTION;
  DELETE FROM {{ env.SCHEMA }}.triggered_email_schedulings
   USING {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging
   WHERE triggered_email_schedulings.ds = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.ds
     AND triggered_email_schedulings.email = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.email
     AND ((triggered_email_schedulings.secondary_id IS NULL AND {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.secondary_id IS NULL) OR triggered_email_schedulings.secondary_id = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.secondary_id)
     AND triggered_email_schedulings.mailing_name = {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging.mailing_name
  ;

  INSERT INTO {{ env.SCHEMA }}.triggered_email_schedulings
  SELECT
    ds
  , cons_id
  , email
  , secondary_id
  , mailing_name
  , GETDATE() AS scheduled_at
    FROM {{ env.MAILING_NAME }}_triggered_email_event_invite_schedulings_staging
  ;
END TRANSACTION;
