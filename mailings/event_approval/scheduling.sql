-- Given ds, returns approval_start_ds, which is ds - 1 day.
CREATE OR REPLACE FUNCTION compute_approval_start_ds(DATE)
RETURNS DATE
IMMUTABLE
AS $$
-- TODO This should be -1; when we first roll this out, it should be no
-- more than as many days past as the last approval.
  SELECT DATEADD(day, -30, $1)::DATE
$$ LANGUAGE sql
;

-- Event ID and next timeslot on or after ds
DROP TABLE IF EXISTS {{ env.SCHEMA }}.events_next_timeslot_event_approval;
CREATE TABLE {{ env.SCHEMA }}.events_next_timeslot_event_approval AS
SELECT
  {{ env.DS | pprint }}::DATE AS ds
, events.id AS event_id
, convert_timezone('America/New_York', MIN(timeslots.start_date)::TIMESTAMP)::DATE AS start_date
, MIN(timeslots.start_date) AS start_timestamp
  FROM mobilizeamerica.events
  JOIN mobilizeamerica.timeslots
    ON events.id = timeslots.event_id
   AND convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP) >= {{ env.DS | pprint }}::DATE
 GROUP BY 1, 2
;

DROP TABLE IF EXISTS {{ env.SCHEMA }}.just_approved_events;
CREATE TABLE {{ env.SCHEMA }}.just_approved_events AS
SELECT
  {{ env.DS | pprint }}::DATE AS ds
, NULL::BIGINT AS cons_id
, events.creator__email_address AS email
, events.id AS secondary_id
, {{ env.MAILING_NAME | pprint }} AS mailing_name
, events.creator__given_name AS firstname
, events.creator__family_name AS lastname
, 'https://events.elizabethwarren.com/event/' || events.id AS event_url
, 'https://events.elizabethwarren.com/dashboard/event/' || events.id AS event_dashboard_url
, events.visibility AS event_visibility
, events.title AS event_title
, events.summary AS event_summary
, events.description AS event_description
, events.event_type
, events.location__venue AS event_venue
, events.location__address_line_1 AS event_addr1
, events.location__locality AS event_city
, events.location__region AS event_state
, events.location__postal_code AS event_zip
, events.location__lat AS latitude
, events.location__lon AS longitude
, events.timezone AS event_timezone
, events_next_timeslot_event_approval.start_date AS event_start_date
, TO_CHAR(events_next_timeslot_event_approval.start_timestamp AT TIME ZONE events.timezone, 'YYYY-MM-DD HH24:MI:SS') AS event_start_timestamp_local
, events.reviewed_date AT TIME ZONE 'UTC' AS event_reviewed_at
  FROM mobilizeamerica.events
  JOIN {{ env.SCHEMA }}.events_next_timeslot_event_approval
    ON events_next_timeslot_event_approval.ds = {{ env.DS | pprint }}::DATE
   AND events_next_timeslot_event_approval.event_id = events.id
 WHERE events.created_by_volunteer_host = '1'
   AND events.approval_status = 'APPROVED'
   AND events.deleted_date IS NULL
   AND events.location__region NOT IN ('IA', 'NH', 'NV', 'SC')
   AND events.reviewed_date >= compute_approval_start_ds({{ env.DS | pprint }}::DATE)
   AND events.reviewed_date >= TIMESTAMPTZ '2019-07-18 20:00:00-04:00'  -- This is when we stopped sending manual emails.
;

DROP TABLE IF EXISTS triggered_email_event_approval_schedulings_staging;
CREATE TEMP TABLE triggered_email_event_approval_schedulings_staging AS
SELECT
  just_approved_events.*
  FROM {{ env.SCHEMA }}.just_approved_events

  -- Don't send more than one mail about a given event (secondary_id is event ID) to a user, ever.
  LEFT OUTER JOIN {{ env.SCHEMA }}.triggered_email_schedulings
    ON triggered_email_schedulings.email = just_approved_events.email
   AND ((triggered_email_schedulings.secondary_id IS NULL AND just_approved_events.secondary_id IS NULL) OR triggered_email_schedulings.secondary_id = just_approved_events.secondary_id)
   AND triggered_email_schedulings.mailing_name = just_approved_events.mailing_name

 WHERE just_approved_events.ds = {{ env.DS | pprint }}::DATE
   AND triggered_email_schedulings.email IS NULL
;

-- Populate triggered_email_payloads_event_approval with all columns.
-- We populate payloads before schedulings to avoid situation where
-- there is a schedulings row but not a payloads row.
BEGIN TRANSACTION;
  DELETE FROM {{ env.SCHEMA }}.triggered_email_payloads_{{ env.MAILING_NAME }}
   USING triggered_email_event_approval_schedulings_staging
   WHERE triggered_email_payloads_{{ env.MAILING_NAME }}.ds = triggered_email_event_approval_schedulings_staging.ds
     AND triggered_email_payloads_{{ env.MAILING_NAME }}.email = triggered_email_event_approval_schedulings_staging.email
     AND ((triggered_email_payloads_{{ env.MAILING_NAME }}.secondary_id IS NULL AND triggered_email_event_approval_schedulings_staging.secondary_id IS NULL) OR triggered_email_payloads_{{ env.MAILING_NAME }}.secondary_id = triggered_email_event_approval_schedulings_staging.secondary_id)
     AND triggered_email_payloads_{{ env.MAILING_NAME }}.mailing_name = triggered_email_event_approval_schedulings_staging.mailing_name
  ;

  INSERT INTO {{ env.SCHEMA }}.triggered_email_payloads_{{ env.MAILING_NAME }}
    SELECT
      ds
    , cons_id
    , email
    , secondary_id
    , mailing_name
    , firstname
    , lastname
    , event_url
    , event_dashboard_url
    , event_visibility
    , event_title
    , event_summary
    , event_description
    , event_type
    , event_venue
    , event_addr1
    , event_city
    , event_state
    , event_zip
    , event_timezone
    , event_start_timestamp_local
    , event_reviewed_at
    , GETDATE() AS payload_prepared_at
      FROM triggered_email_event_approval_schedulings_staging
    ;
END TRANSACTION;

-- Populate triggered_email_schedulings with the first 5 columns.
BEGIN TRANSACTION;
  DELETE FROM {{ env.SCHEMA }}.triggered_email_schedulings
   USING triggered_email_event_approval_schedulings_staging
   WHERE triggered_email_schedulings.ds = triggered_email_event_approval_schedulings_staging.ds
     AND triggered_email_schedulings.email = triggered_email_event_approval_schedulings_staging.email
     AND ((triggered_email_schedulings.secondary_id IS NULL AND triggered_email_event_approval_schedulings_staging.secondary_id IS NULL) OR triggered_email_schedulings.secondary_id = triggered_email_event_approval_schedulings_staging.secondary_id)
     AND triggered_email_schedulings.mailing_name = triggered_email_event_approval_schedulings_staging.mailing_name
  ;

  INSERT INTO {{ env.SCHEMA }}.triggered_email_schedulings
  SELECT
    ds
  , cons_id
  , email
  , secondary_id
  , mailing_name
  , GETDATE() AS scheduled_at
    FROM triggered_email_event_approval_schedulings_staging
  ;
END TRANSACTION;
