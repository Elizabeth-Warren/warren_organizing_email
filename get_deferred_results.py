# Gets deferred results of triggered email sends for a given mailing for a
# given day. For use in tandem with send_triggered.py.

import asyncio
import json
import random
import time

import aiohttp
import pandas as pd

from .utils import sql_quote
from .api import BsdApi
from .send_batch import CONNECTION_CONCURRENCY_LIMIT
from .send_base import (
    DEFERRED_MAILING_RECIPIENT_ID,
    DEFERRED_MAILING_SEND_ID,
    ERROR_MAILING_RECIPIENT_ID,
    ERROR_MAILING_SEND_ID,
    INVAL_MAILING_RECIPIENT_ID,
    INVAL_MAILING_SEND_ID,
    UNKNOWN_MAILING_RECIPIENT_ID,
    UNKNOWN_MAILING_SEND_ID,
    UNSUB_MAILING_RECIPIENT_ID,
    UNSUB_MAILING_SEND_ID,
)

# After sending a batch, we poll BSD til we can get the results of the send of
# each email in the batch. We'll wait this many seconds between each poll.
WAIT_BETWEEN_DEFERRED_RESULTS_POLL = 2


class BsdTriggeredEmailGetDeferredResults:

    # TODO(Jason Katz-Brown): Let's move these to jinja2 files.
    VALUES_SQL = """({ds}, {cons_id}, {email}, {secondary_id}, {mailing_name}, {sent_ds}, GETDATE(), {mailing_id}, {mailing_recipient_id}, {mailing_send_id}, {deferred_task_id})"""

    GET_DEFERMENTS_SQL_MEAT = """
  FROM "{schema}"."triggered_email_sends"
 WHERE "triggered_email_sends".ds = '{ds}'
   AND "triggered_email_sends".mailing_name = '{mailing_name}'
   AND "triggered_email_sends".mailing_recipient_id = {mailing_recipient_id}
   AND "triggered_email_sends".mailing_send_id = {mailing_send_id}
"""

    GET_DEFERMENTS_SQL = """
SELECT
  "triggered_email_sends".*

DEFERMENTSMEATGOESHERE

 ORDER BY "triggered_email_sends".email
     , "triggered_email_sends".secondary_id
     , "triggered_email_sends".sent_at
 LIMIT {batch_size}
;
""".replace(
        "DEFERMENTSMEATGOESHERE", GET_DEFERMENTS_SQL_MEAT
    )

    GET_DEFERMENTS_COUNT_SQL = """
SELECT
  COUNT(*) AS c

DEFERMENTSMEATGOESHERE
;
""".replace(
        "DEFERMENTSMEATGOESHERE", GET_DEFERMENTS_SQL_MEAT
    )

    UPDATE_DEFERMENTS_SQL = """
BEGIN TRANSACTION;

DELETE FROM "{schema}"."triggered_email_sends"
 WHERE ds={ds}
   AND mailing_name={mailing_name}
   AND email IN ({emails_to_delete})
   AND deferred_task_id IN ({deferred_task_ids_to_delete})
   AND mailing_recipient_id={mailing_recipient_id}
   AND mailing_send_id={mailing_send_id}
;

INSERT INTO "{schema}"."triggered_email_sends" (ds, cons_id, email, secondary_id, mailing_name, sent_ds, sent_at, mailing_id, mailing_recipient_id, mailing_send_id, deferred_task_id)
VALUES {values}
;

END TRANSACTION;
"""

    def __init__(
        self,
        civis,
        bsd_host,
        bsd_api_id,
        bsd_secret,
        schema,
        limit_per_mailing,
        batch_size,
    ):
        self.civis = civis
        self.bsd_api = BsdApi(bsd_api_id, bsd_secret, bsd_host, 80, 443)
        self.schema = schema
        self.limit_per_mailing = limit_per_mailing
        self.batch_size = batch_size

        if self.limit_per_mailing and self.batch_size > self.limit_per_mailing:
            print(
                f"Batch size {self.batch_size} larger than limit {self.limit_per_mailing}; setting batch size to {self.limit_per_mailing}"
            )
            self.batch_size = self.limit_per_mailing

    def _should_sample_log(self):
        return random.random() < 0.01

    async def get_deferred_results(self, ds, mailing_name):
        """Gets all deferred results for date 'ds' for mailing 'mailing_name'.

        Fetches in batches of size determined by 'batch_size' passed to
        constructor. All requests to BSD API are attempted concurrently.

        For each row in triggered_email_schedulings with 'DEFER' as
        mailing_recipient_id, makes request to BSD API to get result and fills
        in mailing_recipient_id and mailing_send_id.
        """

        print(f"Getting deferred results of mailing {mailing_name} for ds {ds}.")
        print(f"(Connection concurrency limit: {CONNECTION_CONCURRENCY_LIMIT})")

        count_of_deferred_results = await self._get_deferments_count(ds, mailing_name)
        print(
            f"Will get results for {count_of_deferred_results} deferments in batches of {self.batch_size}."
        )

        loop = asyncio.get_event_loop()
        connector = aiohttp.TCPConnector(limit=CONNECTION_CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
            gotten_in_last_batch = 1
            gotten_total = 0
            batch_i = 0
            while gotten_in_last_batch > 0:
                gotten_in_last_batch = await self._get_deferred_results_batch(
                    session,
                    ds,
                    mailing_name,
                    batch_i,
                    gotten_total,
                    count_of_deferred_results,
                )
                gotten_total += gotten_in_last_batch
                batch_i += 1
                if self.limit_per_mailing and gotten_total >= self.limit_per_mailing:
                    print(f"Hit limit of {self.limit_per_mailing} deferments")
                    break

        print(f"Finished getting {gotten_total} deferred results.")

    async def _get_deferred_results_batch(
        self,
        session,
        ds,
        mailing_name,
        batch_i,
        info_gotten_total,
        info_count_of_deferred_results,
    ):
        # Print debug info only for the first batch.
        debug = batch_i == 0

        # TODO(Jason Katz-Brown): Get these timings into CloudWatch.
        t0 = time.time()
        deferments = await self._get_deferments(ds, mailing_name, debug)

        if len(deferments) == 0:
            print("No deferments to get results for.")
            return 0

        t1 = time.time()
        time_get_deferments = t1 - t0

        deferments_with_results = [
            {"deferment": deferment} for deferment in deferments.itertuples()
        ]

        await self._get_deferred_results_async(session, deferments_with_results, debug)

        t2 = time.time()
        time_get_deferred_results = t2 - t1

        await self._replace_deferments(ds, mailing_name, deferments_with_results, debug)

        t3 = time.time()
        time_replace_deferments = t3 - t2

        time_total = t3 - t0
        num_sent = len(deferments_with_results)

        print(
            f"Got {info_gotten_total + num_sent}/{info_count_of_deferred_results} deferred results in {time_total}. "
            f"Timings: get_deferments {time_get_deferments:.2f}; "
            f"get_deferred_results {time_get_deferred_results:.2f}; "
            f"replace_deferments {time_replace_deferments:.2f}; "
        )

        return num_sent

    async def _get_deferred_results_async(
        self, session, deferments_with_results, debug
    ):
        results = await asyncio.gather(
            *[
                self._get_deferred_result_async(
                    session, deferment_with_result["deferment"], debug
                )
                for deferment_with_result in deferments_with_results
            ],
            return_exceptions=True,
        )
        for deferment_with_result, result in zip(deferments_with_results, results):
            if isinstance(result, Exception):
                print("Got exception from get_deferred_result_async", result)
            else:
                deferment_with_result.update(result)

    async def _get_deferred_result_async(self, session, deferment, debug):
        deferred_task_id = deferment.deferred_task_id
        status = 202
        body = ""
        while status == 202 or "Rate limit exceeded" in body:
            await asyncio.sleep(WAIT_BETWEEN_DEFERRED_RESULTS_POLL)
            if debug and self._should_sample_log():
                print(
                    f"process_triggered_email_api_response_async({deferred_task_id}) making followup."
                )
            status, body = await self.bsd_api.getDeferredResults(
                session, deferred_task_id
            )

        if "This e-mail address is unsubscribed and cannot be delivered to" in body:
            mailing_recipient_id = UNSUB_MAILING_RECIPIENT_ID
            mailing_send_id = UNSUB_MAILING_SEND_ID
            deferred_task_id = None
        elif "Must be a valid e-mail address" in body:
            mailing_recipient_id = INVAL_MAILING_RECIPIENT_ID
            mailing_send_id = INVAL_MAILING_SEND_ID
            deferred_task_id = None
        elif "The requested deferred result has already been delivered" in body:
            # This must be the result of a bug; we fetched deferred result but
            # then failed to store it in DB.
            mailing_recipient_id = UNKNOWN_MAILING_RECIPIENT_ID
            mailing_send_id = UNKNOWN_MAILING_SEND_ID
            deferred_task_id = None
        else:
            result = None
            try:
                result = json.loads(body)
            except json.decoder.JSONDecodeError:
                print("Got non-JSON response:", body)
                print("Status was", status)

            if (
                result
                and "mailing_recipient_id" in result
                and "mailing_send_id" in result
            ):
                mailing_recipient_id = result["mailing_recipient_id"]
                mailing_send_id = result["mailing_send_id"]
                deferred_task_id = None
            else:
                print("Bailing out and marking as error. Response body was:", body)
                mailing_recipient_id = ERROR_MAILING_RECIPIENT_ID
                mailing_send_id = ERROR_MAILING_SEND_ID
                deferred_task_id = None

        return {
            "mailing_recipient_id": mailing_recipient_id,
            "mailing_send_id": mailing_send_id,
            "deferred_task_id": deferred_task_id,
        }

    async def _replace_deferments(
        self, ds, mailing_name, deferments_with_results, debug
    ):
        deferred_task_ids_to_delete = []
        emails_to_delete = []
        values = []
        for deferment_with_result in deferments_with_results:
            deferment = deferment_with_result["deferment"]
            deferred_task_ids_to_delete.append(sql_quote(deferment.deferred_task_id))
            emails_to_delete.append(sql_quote(deferment.email))

            mailing_recipient_id = deferment_with_result["mailing_recipient_id"]
            mailing_send_id = deferment_with_result["mailing_send_id"]
            if (
                mailing_recipient_id == ERROR_MAILING_RECIPIENT_ID
                or mailing_send_id == ERROR_MAILING_SEND_ID
            ):
                # We'll delete the placeholder rows that resulted in an error.
                # This relies on assumption that all such errors are transient.
                continue

            deferred_task_id = deferment_with_result["deferred_task_id"]
            values.append(
                self.VALUES_SQL.format(
                    ds=sql_quote(deferment.ds),
                    cons_id=sql_quote(deferment.cons_id),
                    email=sql_quote(deferment.email),
                    secondary_id=sql_quote(deferment.secondary_id),
                    mailing_name=sql_quote(deferment.mailing_name),
                    sent_ds=sql_quote(deferment.sent_ds),
                    mailing_id=sql_quote(deferment.mailing_id),
                    mailing_recipient_id=sql_quote(mailing_recipient_id),
                    mailing_send_id=sql_quote(mailing_send_id),
                    deferred_task_id=sql_quote(deferred_task_id),
                )
            )
        sql = self.UPDATE_DEFERMENTS_SQL.format(
            schema=self.schema,
            ds=sql_quote(ds),
            mailing_name=sql_quote(mailing_name),
            emails_to_delete=", ".join(emails_to_delete),
            deferred_task_ids_to_delete=", ".join(deferred_task_ids_to_delete),
            mailing_recipient_id=sql_quote(DEFERRED_MAILING_RECIPIENT_ID),
            mailing_send_id=sql_quote(DEFERRED_MAILING_SEND_ID),
            values=",\n".join(values),
        )

        if debug:
            print(f"Replacing deferments with SQL: {sql[:10000]} (truncated)")

        await self.civis.run_sql(sql)

    async def _get_deferments_count(self, ds, mailing_name):
        query = self.GET_DEFERMENTS_COUNT_SQL.format(
            ds=ds,
            mailing_name=mailing_name,
            mailing_recipient_id=sql_quote(DEFERRED_MAILING_RECIPIENT_ID),
            mailing_send_id=sql_quote(DEFERRED_MAILING_SEND_ID),
            schema=self.schema,
        )
        df = await self.civis.read_civis_sql(query)
        return df["c"].iloc[0]

    async def _get_deferments(self, ds, mailing_name, debug):
        query = self.GET_DEFERMENTS_SQL.format(
            ds=ds,
            mailing_name=mailing_name,
            mailing_recipient_id=sql_quote(DEFERRED_MAILING_RECIPIENT_ID),
            mailing_send_id=sql_quote(DEFERRED_MAILING_SEND_ID),
            schema=self.schema,
            batch_size=self.batch_size,
        )
        if debug:
            print(f"get_deferments query: {query}")
        df = await self.civis.read_civis_sql(query)

        return df
