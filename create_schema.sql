-- Creates schedulings and sends table for triggered emails.
-- Inputs variables: 'env.SCHEMA'

DROP TABLE IF EXISTS {{ env.SCHEMA }}."triggered_email_schedulings";
CREATE TABLE {{ env.SCHEMA }}."triggered_email_schedulings" (
    -- ds is the date which we schedule the email to be sent. This abets
    -- backfilling and forecasting for mailings where it makes sense to
    -- schedule them in advance.
    "ds" DATE ENCODE RAW NULL,

    -- Intended recipient. cons_id is set for mails to our list (e.g.
    -- event invites), and can be NULL for emails separate from our
    -- list (e.g. event approval).
    "cons_id" BIGINT ENCODE lzo NULL,
    "email" CHARACTER VARYING(1024) ENCODE lzo NULL DISTKEY,

    -- secondary_id allows codifying that this email is about a certain
    -- external object. This is useful if you may want to send the same
    -- mailing to the same person more than once in a day, as long as
    -- the secondary_id of the two mails are different.
    "secondary_id" BIGINT ENCODE lzo NULL,

    -- The mailing name (e.g. event_approval, event_invite, lead_onboarding).
    "mailing_name" CHARACTER VARYING(1024) ENCODE lzo NULL,

    -- When this row was inserted.
    "scheduled_at" TIMESTAMP ENCODE RAW NULL
)
DISTSTYLE KEY
SORTKEY ("ds", "email");

DROP TABLE IF EXISTS {{ env.SCHEMA }}."triggered_email_sends";
CREATE TABLE {{ env.SCHEMA }}."triggered_email_sends" (
    -- These fields exactly match those of triggered_email_schedulings.
    "ds" DATE ENCODE RAW NULL,
    "cons_id" BIGINT ENCODE lzo NULL,
    "email" CHARACTER VARYING(1024) ENCODE lzo NULL DISTKEY,
    "secondary_id" BIGINT ENCODE lzo NULL,
    "mailing_name" CHARACTER VARYING(1024) ENCODE lzo NULL,

    -- Date and approximate time of sending. (sent_ds should equal ds,
    -- but may not in the case of a backfill send.)
    "sent_ds" DATE ENCODE RAW NULL,
    "sent_at" TIMESTAMP ENCODE RAW NULL,

    -- BSD triggered email template mailing ID, e.g. 'U1cOBQ'.
    "mailing_id" CHARACTER VARYING(1024) ENCODE lzo NULL,

    -- These are filled in from BSD triggered email API response. They
    -- function as receipt that BSD processed the send request.
    "mailing_recipient_id" CHARACTER VARYING(1024) ENCODE lzo NULL,
    "mailing_send_id" CHARACTER VARYING(1024) ENCODE lzo NULL,

    -- All BSD triggered email API send requests return a
    -- deferred_task_id. We store this, and at a later point, fetch the
    -- actual result.
    "deferred_task_id" CHARACTER VARYING(256) ENCODE lzo NULL
)
DISTSTYLE KEY
SORTKEY ("ds", "email");
