# Automated organizing email on Warren for President

*Questions? Want help setting something like this up for your own campaign?
Reach out to [Jason](https://www.linkedin.com/in/jasonkatzbrown/): jasonkatzbrown@gmail.com*

Our Mobilization and Tech teams worked together to scale email outreach to the widest possible audience and free our incredible organizers from tedious manual tasks. For example, we set up an automated daily event invite mailing that recruited tens of thousands of volunteer shifts, and an automated mailing to event hosts that onboarded thousands of event hosts. We are grateful down to our toes for the hundreds of thousands of Warren supporters who used these emails to help our grassroots movement -- here's some technical behind-the-scenes!

## Background

Warren for President had a world-class email organizing team. A lot of
the program relied on personal touch. And there were opportunities to
automate some mailings to enable them to scale to millions of
supporters across every state. Grassroots Mobilization and Tech teams
worked hand in hand to take programs that worked really well and
automate them where possible to be able to reach the widest possible
audience and free our incredible teammates from or tedious manual tasks.

This repository gives a taste of some of the workflows we set up to
automatically prepare or send email for organizing.

## Goal

Our goal was to take successful manually-built emails
and be able to send them in an automated way, at high volume, with full
personalization, at optimal timing to each recipient. We focused on
types of mailings where sending manually would require lots of
teammates' time, could not be personalized enough, be incredibly tedious, be error
prone, or be simply impossible.

On the implementation side, we aimed to achieve these architectural design goals:

- keep a record of all emails sent
- be able to quality-control future sends before they were actually sent
- monitor trends and be able to answer questions like "how many people did we invite to event X?"
- share as much code and infrastructure across all types of emails

We designed a set of parameterized jobs that could be scheduled into
workflows (e.g. Civis or Airflow) to schedule and send automated
organizing email. The rest of this document we'll walk through how this
looked for two example emails.

## Onboarding volunteer event hosts

The first use case of this email pipeline was personalized onboarding
for new volunteer event hosts. Leading up to the first debate,
we succeeded in recruiting a thousand volunteers to host
debate watch parties, in every state and Puerto Rico! Regrettably we had
to personally email each of these thousand hosts onboarding materials,
like invites to trainings and handouts. This took hundreds of hours and
the distributed events team didn't sleep all week! For debate two, we
were determined to automate the event host onboarding email.

The first automated organizing email was an email to event hosts whose
Mobilize America event had just been approved. The workflow ran every
hour and looked like this:

1. Make a row in the `triggered_email_schedulings` table for each
   approved event for which we haven't emailed onboarding materials yet.
2. Make a row in the `triggered_email_payloads_event_approval` table
   with personalization data (e.g. event name, date, link to Mobilize
   America dashboard) for each recipient.
3. Update a Google Spreadsheet with the recipients we were about to
   email and the details of the event.
4. For each row of `triggered_email_schedulings` table, use BSD
   triggered email API to send email with payload from
   `triggered_email_payloads_event_approval`, and store the status of the
   send in `triggered_email_schedulings` table, one row per recipient per
   day.

We turned this on for every future debate -- no more manual mailing of
onboarding materials! And we could personalize the mailing itself to
more types of events, to the state of the host, or anything else the
distributed events team dreamed.

<kbd>
<img src="https://www.dropbox.com/s/jga8xi5ooslqwcn/Screenshot%202020-03-12%2000.40.15.png?raw=1" height=400>
</kbd>

## Inviting supporters to events

Next, we turned our attention to event invites. Our supporters were
stepping up all over the country to host all sorts of incredible events
and we wanted to help get people into the fight and to these events!

We started with a manually-built email every Wednesday. This
would invite each supporter within 25 miles of an event to their
soonest upcoming event. This worked great and drew many shifts, but there was
room to improve:

- Since it always invited to the soonest event, it might invite to an
  event on Thursday or Friday, event when there was a nearer event or a
  more convenient event on the weekend.
- Since it always went out on Wednesday, it might give too little lead
  time (for Thursday event) or too much lead time (for event next
  Tuesday).
- Since it only went out once a week, we couldn't invite to more than
  one event per week.
- We couldn't answer questions like "How many people will we invite to
  Sunday's community canvass in Richmond?" or "How many people _did_ we invite to
  that community canvass?"
- It could take almost all day on Wednesday to prepare and send and
  monitor this email.

We all put our heads together and worked on a way to automatically send
event invites to our distributed events every day:

- Send event invites every day (not just Wednesday).
- Each recipient gets a 3-day quiet period after receiving an event invite.
- Send invites 3 days before the event.
- Within a sliding time window, prefer closer events and events on the
  weekend.
- Don't send invites to an event if it's already close to maximum
  capacity and there would be more than 5000 invitees per available seat

We rolled this out one state at a time. We started in Florida -- first
we excluded Florida recipients from the Wednesday email, and sent
invites to Florida with the new automated workflow, after verifying
that the volume of invites sent matched what the volume of Wednesday
email would have been. After a few weeks, we saw shift recruitment rates
significantly improved from past weeks in Florida, and we ramped up to 3
states, then to all non-early states.

We sent this automated daily event invite mail for all non-early-state,
non-canvass events for the last four months of the campaign. It
recruited tens of thousands of shift signups.

In addition to the daily event invite email for non-canvass events, we
also genericized the event invite pipeline to support all kinds of event
invite emails, like canvass- or GOTV-specific nationwide mailings. For
these, we tended to send these mailings with a hybrid approach: an automated
workflow would produce a BSD constituent group and personalization
dataset and a teammate would prepare the template and press send.

<kbd>
<img src="https://www.dropbox.com/s/j3z1fcj9au4gm50/Screenshot%202020-03-11%2023.55.29.png?raw=1" height=300>
</kbd>

## And more!

This framework supported all sorts of automated organizing needs as they
came up.

We set up an email triggered when a supporter answered "yes" to
a volunteer ask across any channel (Reach, online form, texting
VOLUNTEER to 24477, ...), personalized to the type of volunteering they
wanted. For example, if somebody answered Yes to door knocking on Reach,
our mobile canvassing app, they'd get an email about how to start
canvassing. This hot-leads email recruited hundreds of shift signups.

<kbd>
<img src="https://www.dropbox.com/s/yy5pm4k7fkwo2ql/Screenshot%202019-12-05%2013.44.55.png?raw=1" height=400>
</kbd>

---

We pulled out all the stops for GOTV. We set up an automated daily email to everybody signed up
for a GOTV shift the next day, based on signups and email addresses in
VAN. This contributed to incredibly low flake rates. For recruitment
into GOTV shifts, we tried a bunch of things. One experimental success:
including buttons for every GOTV shift, each linked to sign up to the _specific
shift_, significantly increased signups.

<kbd>
<img src="https://www.dropbox.com/s/hjoj0i14seom0zf/Screenshot%202020-02-24%2021.20.48.png?raw=1" height=300>
</kbd>

---

Our automated email efforts spanned all our products. We optionally sent
you an email after you looked up your caucus or voting location on
elizabethwarren.com:

<kbd>
<img src="https://www.dropbox.com/s/sk0z55x83hp5ww0/Screenshot%202020-02-15%2013.08.55.png?raw=1" height=400>
</kbd>

---

And delivered your link to our Grassroots Donor Wall:

<kbd>
<img src="https://www.dropbox.com/s/56f8g98arl2kx0p/Screenshot%202020-03-12%2001.56.14.png?raw=1" height=300>
</kbd>

## In conclusion

On the Tech team, we aimed to empower everybody on Warren for President with technology to support them in the fight. We helped scale organizing programs to reach hundreds of thousands of volunteers and millions of voters. We are so grateful for the hundreds of thousands of Warren supporters who used these emails to help our grassroots movement: thank you.

*Questions? Want help setting something like this up for your own campaign?
Reach out to [Jason](https://www.linkedin.com/in/jasonkatzbrown/): jasonkatzbrown@gmail.com*

### Appendix: In-the-weeds technical details

What follows is an introduction to the code in this repo. It is not
straightforwardly runnable out of the box, but hopefully can be used as
inspiration for your own email programs!

Each type of mailing has a `mailing_name`; our two examples are
`event_invite` and `event_approval`.

Code specific to an individual mailing lives in `mailings/{mailing_name}`.

Inside a mailing’s directory, there are two pieces:

- scheduling.sql: Creates one row in `triggeredemail.triggered_email_schedulings` per  email to send
- payloads_create_schema.sql: Defines schema that holds the mailing’s payload variables

#### `triggered_email_schedulings` table

Schemas is defined in top-level `create_schema.sql`:

```
CREATE TABLE "triggeredemail"."triggered_email_schedulings" (
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
```

This table is populated by the first task in a Civis or Airflow workflow that schedules, forecasts, and sends a mailing every day (or every hour). The typical workflow would chain three jobs together: To schedule the mail for today (running `mailing/.../scheduling.sql`), to prepare the forecast spreadsheets (calling into forecast.py), then to send the mail (calling into `send_(triggered|recurring|ses).py`).

#### `triggered_email_sends` table

Also from `create_schema.sql`:

```
CREATE TABLE "triggeredemail"."triggered_email_sends" (
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
```

This table is populated by `send_triggered.py`: First it selects a batch of rows from `triggered_email_schedulings`; then it creates a corresponding new row for each with placeholder mailing_recipient_id; then it sends the mail; then when the deferred BSD API result is ready, updates with the actual mailing_recipient_id.

Nontransactional mails are higher volume and not good for BSD triggered email API. For these, we send once per day with `send_recurring.py`, and use a BSD recurring mailing. Finally, we also experimented with sending emails with SES (`send_ses.py`).

#### Forecasting

Some mails are transactional -- we send them as soon as possible after a user action (event approval, vol-yes canvass, or etc in the future). Some mails are promotional and nontransactional -- we send them to large swathes of our list. For these, we must be careful in how we time them.

To help in planning, our nontransactional scheduling workflows schedule for today and each of the next seven days. For example, this workflow populates rows in `triggered_email_schedulings` for `ds` of today’s date, and `ds` of each of the next seven days. (When we actually send mails, we send only for `ds` of today’s date.)

That workflow also runs this forecasting program to populate the Event Invite Email Forecast listed above, which lists schedulings by day. (Nontransactional scheduling and forecasting workflows also populate such a spreadsheet, but for nontransactional mailings, we only populate the sheet corresponding to today’s date.)
