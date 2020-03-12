import pandas as pd
from nameparser import HumanName

from .utils import sql_quote

PLACEHOLDER_MAILING_RECIPIENT_ID = "PLACE"
PLACEHOLDER_MAILING_SEND_ID = "PLACE"

DEFERRED_MAILING_RECIPIENT_ID = "DEFER"
DEFERRED_MAILING_SEND_ID = "DEFER"

ERROR_MAILING_RECIPIENT_ID = "ERROR"
ERROR_MAILING_SEND_ID = "ERROR"

UNSUB_MAILING_RECIPIENT_ID = "UNSUB"
UNSUB_MAILING_SEND_ID = "UNSUB"

INVAL_MAILING_RECIPIENT_ID = "INVAL"
INVAL_MAILING_SEND_ID = "INVAL"

UNKNOWN_MAILING_RECIPIENT_ID = "UNKNO"
UNKNOWN_MAILING_SEND_ID = "UNKNO"


class SendBase:
    GET_SCHEDULINGS_SQL_MEAT = """
  FROM "{schema}"."triggered_email_schedulings"
  JOIN "{schema}"."triggered_email_payloads_{mailing_name}"
    ON "triggered_email_payloads_{mailing_name}".ds = '{ds}'
   AND (("triggered_email_payloads_{mailing_name}".cons_id IS NULL AND "triggered_email_schedulings".cons_id IS NULL)
        OR "triggered_email_payloads_{mailing_name}".cons_id = "triggered_email_schedulings".cons_id)
   AND "triggered_email_payloads_{mailing_name}".email = "triggered_email_schedulings".email
   AND (("triggered_email_payloads_{mailing_name}".secondary_id IS NULL AND "triggered_email_schedulings".secondary_id IS NULL)
        OR "triggered_email_payloads_{mailing_name}".secondary_id = "triggered_email_schedulings".secondary_id)
  LEFT OUTER JOIN "{schema}"."triggered_email_sends"
    ON "triggered_email_sends".ds = '{ds}'
   AND "triggered_email_sends".email = "triggered_email_schedulings".email
   AND (("triggered_email_sends".secondary_id IS NULL AND "triggered_email_schedulings".secondary_id IS NULL)
        OR "triggered_email_sends".secondary_id = "triggered_email_schedulings".secondary_id)
   AND "triggered_email_sends".mailing_name = "triggered_email_schedulings".mailing_name
 WHERE "triggered_email_schedulings".ds = '{ds}'
   AND "triggered_email_schedulings".mailing_name = '{mailing_name}'
   AND "triggered_email_sends".email IS NULL

   -- TODO(Jason Katz-Brown): Civis API breaks for long queries containing
   -- non-ASCII characters. We'll ignore email addresses with non-ASCII characters
   -- for now.
   AND REGEXP_INSTR(triggered_email_schedulings.email, '[^[:print:][:cntrl:]]') = 0
"""

    GET_SCHEDULINGS_SQL = """
SELECT
  "triggered_email_schedulings".scheduled_at
, "triggered_email_payloads_{mailing_name}".*

SCHEDULINGSMEATGOESHERE

 ORDER BY "triggered_email_schedulings".email
     , "triggered_email_schedulings".secondary_id
     , "triggered_email_schedulings".scheduled_at
 LIMIT {batch_size}
;
""".replace(
        "SCHEDULINGSMEATGOESHERE", GET_SCHEDULINGS_SQL_MEAT
    )

    GET_SCHEDULINGS_COUNT_SQL = """
SELECT
  COUNT(*) AS c

SCHEDULINGSMEATGOESHERE
;
""".replace(
        "SCHEDULINGSMEATGOESHERE", GET_SCHEDULINGS_SQL_MEAT
    )

    DELETE_SENDS_SQL = """
DELETE FROM "{schema}"."triggered_email_sends"
 WHERE ds={ds}
   AND mailing_name={mailing_name}
   AND email IN ({emails_to_delete})
   AND mailing_id={mailing_id}
   AND mailing_recipient_id={mailing_recipient_id}
   AND mailing_send_id={mailing_send_id}
;
"""

    INSERT_SEND_SQL = """INSERT INTO "{schema}"."triggered_email_sends" (ds, cons_id, email, secondary_id, mailing_name, sent_ds, sent_at, mailing_id, mailing_recipient_id, mailing_send_id, deferred_task_id)
VALUES {values}
;
"""

    VALUES_SQL = """({ds}, {cons_id}, {email}, {secondary_id}, {mailing_name}, {sent_ds}, GETDATE(), {mailing_id}, {mailing_recipient_id}, {mailing_send_id}, {deferred_task_id})"""

    UPDATE_SENDS_SQL = """
BEGIN TRANSACTION;

DELETESTATEMENTGOESHERE

INSERTSTATEMENTGOESHERE

END TRANSACTION;
""".replace(
        "DELETESTATEMENTGOESHERE", DELETE_SENDS_SQL
    ).replace(
        "INSERTSTATEMENTGOESHERE", INSERT_SEND_SQL
    )

    # Same as DELETE_SENDS_SQL, but also restricts to given deferred_task_ids.
    DELETE_DEFERMENTS_SQL = """
DELETE FROM "{schema}"."triggered_email_sends"
 WHERE ds={ds}
   AND mailing_name={mailing_name}
   AND email IN ({emails_to_delete})
   AND deferred_task_id IN ({deferred_task_ids_to_delete})
   AND mailing_recipient_id={mailing_recipient_id}
   AND mailing_send_id={mailing_send_id}
;
"""

    UPDATE_DEFERMENTS_SQL = """
BEGIN TRANSACTION;

DELETESTATEMENTGOESHERE

INSERTSTATEMENTGOESHERE

END TRANSACTION;
""".replace(
        "DELETESTATEMENTGOESHERE", DELETE_DEFERMENTS_SQL
    ).replace(
        "INSERTSTATEMENTGOESHERE", INSERT_SEND_SQL
    )

    DELETE_ALL_SENDS_SQL = """
DELETE FROM "{schema}"."triggered_email_sends"
 WHERE ds={ds}
   AND mailing_name={mailing_name}
   AND mailing_id={mailing_id}
   AND mailing_recipient_id={mailing_recipient_id}
   AND mailing_send_id={mailing_send_id}
;
"""

    def __init__(self, civis, schema, limit_per_mailing, batch_size):
        self.civis = civis
        self.schema = schema
        self.limit_per_mailing = limit_per_mailing
        self.batch_size = batch_size

    async def _get_schedulings_count(self, ds, mailing_name):
        query = self.GET_SCHEDULINGS_COUNT_SQL.format(
            ds=ds, mailing_name=mailing_name, schema=self.schema
        )
        df = await self.civis.read_civis_sql(query)
        return df["c"].iloc[0]

    async def _get_schedulings(self, ds, mailing_name, debug):
        query = self.GET_SCHEDULINGS_SQL.format(
            ds=ds,
            mailing_name=mailing_name,
            schema=self.schema,
            batch_size=self.batch_size,
        )
        if debug:
            print(f"get_schedulings query: {query}")
        df = await self.civis.read_civis_sql(query)

        return df

    async def _insert_placeholder_sends(
        self,
        schedulings,
        mailing_id,
        sent_ds,
        debug,
        placeholder_mailing_recipient_id=PLACEHOLDER_MAILING_RECIPIENT_ID,
        placeholder_mailing_send_id=PLACEHOLDER_MAILING_SEND_ID,
    ):
        values = []
        for scheduling in schedulings.itertuples(index=False):
            values.append(
                self.VALUES_SQL.format(
                    ds=sql_quote(scheduling.ds),
                    cons_id=sql_quote(scheduling.cons_id),
                    email=sql_quote(scheduling.email),
                    secondary_id=sql_quote(scheduling.secondary_id),
                    mailing_name=sql_quote(scheduling.mailing_name),
                    sent_ds=sql_quote(sent_ds),
                    mailing_id=sql_quote(mailing_id),
                    mailing_recipient_id=sql_quote(placeholder_mailing_recipient_id),
                    mailing_send_id=sql_quote(placeholder_mailing_send_id),
                    deferred_task_id=sql_quote(None),
                )
            )
        sql = self.INSERT_SEND_SQL.format(schema=self.schema, values=",\n".join(values))

        if debug:
            print(f"Inserting placeholder sends with SQL: {sql[:10000]} (truncated)")

        await self.civis.run_sql(sql)

    async def _replace_placeholder_sends(
        self, ds, mailing_name, mailing_id, schedulings_with_send_status, debug
    ):
        emails_to_delete = []
        values = []
        for scheduling_with_send_status in schedulings_with_send_status:
            scheduling = scheduling_with_send_status["scheduling"]

            emails_to_delete.append(sql_quote(scheduling.email))

            mailing_recipient_id = scheduling_with_send_status["mailing_recipient_id"]
            mailing_send_id = scheduling_with_send_status["mailing_send_id"]
            if (
                mailing_recipient_id == ERROR_MAILING_RECIPIENT_ID
                or mailing_send_id == ERROR_MAILING_SEND_ID
            ):
                # We'll delete the placeholder rows that resulted in an error.
                # This relies on assumption that all such errors are transient.
                continue

            deferred_task_id = scheduling_with_send_status["deferred_task_id"]
            sent_ds = scheduling_with_send_status["sent_ds"]
            values.append(
                self.VALUES_SQL.format(
                    ds=sql_quote(scheduling.ds),
                    cons_id=sql_quote(scheduling.cons_id),
                    email=sql_quote(scheduling.email),
                    secondary_id=sql_quote(scheduling.secondary_id),
                    mailing_name=sql_quote(scheduling.mailing_name),
                    sent_ds=sql_quote(sent_ds),
                    mailing_id=sql_quote(mailing_id),
                    mailing_recipient_id=sql_quote(mailing_recipient_id),
                    mailing_send_id=sql_quote(mailing_send_id),
                    deferred_task_id=sql_quote(deferred_task_id),
                )
            )
        sql = self.UPDATE_SENDS_SQL.format(
            schema=self.schema,
            ds=sql_quote(ds),
            mailing_name=sql_quote(mailing_name),
            emails_to_delete=", ".join(emails_to_delete),
            mailing_id=sql_quote(mailing_id),
            mailing_recipient_id=sql_quote(PLACEHOLDER_MAILING_RECIPIENT_ID),
            mailing_send_id=sql_quote(PLACEHOLDER_MAILING_SEND_ID),
            values=",\n".join(values),
        )

        if debug:
            print(f"Replacing placeholder sends with SQL: {sql[:10000]} (truncated)")

        await self.civis.run_sql(sql)

    async def _delete_all_sends_for_ds(
        self, ds, mailing_name, mailing_id, mailing_recipient_id, mailing_send_id, debug
    ):
        query = self.DELETE_ALL_SENDS_SQL.format(
            schema=self.schema,
            ds=sql_quote(ds),
            mailing_name=sql_quote(mailing_name),
            mailing_id=sql_quote(mailing_id),
            mailing_recipient_id=sql_quote(mailing_recipient_id),
            mailing_send_id=sql_quote(mailing_send_id),
        )
        if debug:
            print(f"delete_all_sends_for_ds query: {query}")
        await self.civis.run_sql(query)

    def _massage_payload(self, payload):
        for k, v in payload.items():
            if pd.isnull(v) or not v:
                # Replace nan or None with empty string.
                payload[k] = ""

        # Ensure names aren't all caps or all lowercase.
        if payload.get("firstname") and payload.get("lastname"):
            name = HumanName()
            name.first = payload["firstname"]
            name.last = payload["lastname"]
            name.capitalize()
            payload["firstname"] = name.first
            payload["lastname"] = name.last
