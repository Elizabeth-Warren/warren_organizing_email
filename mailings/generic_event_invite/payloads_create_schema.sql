DROP TABLE IF EXISTS {{ env.SCHEMA }}."triggered_email_payloads_{{ env.MAILING_NAME }}";
CREATE TABLE {{ env.SCHEMA }}."triggered_email_payloads_{{ env.MAILING_NAME }}" (
    "ds" DATE ENCODE RAW NULL,
    "cons_id" BIGINT ENCODE lzo NULL,
    "email" CHARACTER VARYING(1024) ENCODE lzo NULL DISTKEY,
    "secondary_id" BIGINT ENCODE lzo NULL,
    "mailing_name" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "cons_city" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "cons_state" CHARACTER VARYING(32) ENCODE RAW NULL,
    "cons_zip" CHARACTER VARYING(10) ENCODE RAW NULL,
    "distance_to_event" NUMERIC(10, 2) ENCODE RAW NULL,
    "event_url" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_title" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_summary" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_type" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_venue" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_addr1" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_city" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_state" CHARACTER VARYING(32) ENCODE RAW NULL,
    "event_zip" CHARACTER VARYING(10) ENCODE RAW NULL,
    "event_timezone" CHARACTER VARYING(100) ENCODE RAW NULL,
    "event_start_timestamp_local" CHARACTER VARYING(100) ENCODE RAW NULL,
    "payload_prepared_at" TIMESTAMP ENCODE RAW NULL,
    "cons_state_name" CHARACTER VARYING(256) NULL,
    "firstname" CHARACTER VARYING(255) ENCODE RAW NULL,
    "lastname" CHARACTER VARYING(255) ENCODE RAW NULL,
    "event_start_formatted_date_local" CHARACTER VARYING(255) ENCODE RAW NULL,
    "event_start_formatted_time_local" CHARACTER VARYING(255) ENCODE RAW NULL,
    "event_start_day_of_week_local" CHARACTER VARYING(255) ENCODE RAW NULL,
    "event_formatted_start_times" CHARACTER VARYING(65535) ENCODE RAW NULL,
    "event_csv_timeslot_urls" CHARACTER VARYING(65535) ENCODE RAW NULL,
    "event_csv_start_times" CHARACTER VARYING(65535) ENCODE RAW NULL
)
DISTSTYLE KEY
SORTKEY ("ds", "email")
;
