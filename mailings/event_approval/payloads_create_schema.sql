DROP TABLE IF EXISTS {{ env.SCHEMA }}."triggered_email_payloads_event_approval";
CREATE TABLE {{ env.SCHEMA }}."triggered_email_payloads_event_approval" (
    "ds" DATE ENCODE RAW NULL,
    "cons_id" BIGINT ENCODE lzo NULL,
    "email" CHARACTER VARYING(1024) ENCODE lzo NULL DISTKEY,
    "secondary_id" BIGINT ENCODE lzo NULL,
    "mailing_name" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "firstname" CHARACTER VARYING(255) ENCODE RAW NULL,
    "lastname" CHARACTER VARYING(255) ENCODE RAW NULL,
    "event_url" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_dashboard_url" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_visibility" CHARACTER VARYING(65535) ENCODE RAW NULL,
    "event_title" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_summary" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "event_description" CHARACTER VARYING(MAX) ENCODE lzo NULL,
    "event_type" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_venue" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_addr1" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_city" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_state" CHARACTER VARYING(32) ENCODE RAW NULL,
    "event_zip" CHARACTER VARYING(1024) ENCODE RAW NULL,
    "event_timezone" CHARACTER VARYING(100) ENCODE RAW NULL,
    "event_start_timestamp_local" CHARACTER VARYING(100) ENCODE RAW NULL,
    "event_reviewed_at" TIMESTAMP ENCODE RAW NULL,
    "payload_prepared_at" TIMESTAMP ENCODE RAW NULL
)
DISTSTYLE KEY
SORTKEY ("ds", "email");
