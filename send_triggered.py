# Sends scheduled triggered emails for a given mailing for a given day by
# making requests to BSD triggered emails API. For use in tandem with
# get_deferred_results.py.

import asyncio
import json
import random
import time
import urllib

import aiohttp
import pandas as pd

from .utils import ds_today
from .api import BsdApi
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
    SendBase,
)

# Have at most this many TCP connections open at one time.
# If batch size is 1000, the container tends to crash if we are polling
# deferred results repeatedly for 1000 results all at once. So we limit
# concurrent requests to a bit less.
CONNECTION_CONCURRENCY_LIMIT = 100

# If CONNECTION_CONCURRENCY_LIMIT, we'll also sleep between each API call to be
# extra cautious.
SLEEP_BETWEEN_CONCURRENT_MAILS = 2

# Include this in the placeholder email address to insert what number message
# we're on. Useful for doing an artificial stress test.
OVERRIDE_EMAIL_N_PLACEHOLDER = "{{n}}"

# After sending a batch, we poll BSD til we can get the results of the send of
# each email in the batch. We'll wait this many seconds between each poll.
WAIT_BETWEEN_DEFERRED_RESULTS_POLL = 2


class BsdTriggeredEmailSendBatch(SendBase):
    def __init__(
        self,
        civis,
        bsd_host,
        bsd_api_id,
        bsd_secret,
        schema,
        override_email,
        cc_emails,
        limit_per_mailing,
        batch_size,
        email_whitelist,
    ):
        super().__init__(civis, schema, limit_per_mailing, batch_size)
        self.bsd_api = BsdApi(bsd_api_id, bsd_secret, bsd_host, 80, 443)
        self.cc_emails = cc_emails

        self.override_email = override_email
        if self.override_email:
            self.override_email = self.override_email.split(",")

        if self.limit_per_mailing and self.batch_size > self.limit_per_mailing:
            print(
                f"Batch size {self.batch_size} larger than limit {self.limit_per_mailing}; setting batch size to {self.limit_per_mailing}"
            )
            self.batch_size = self.limit_per_mailing

        self.email_whitelist = email_whitelist

    async def send(self, ds, mailing_name, mailing_id, count_first=True):
        """Sends all scheduled mails for date 'ds' for mailing 'mailing_name'.

        'mailing_id' is the BSD mailing id corresponding to 'mailing_name', e.g. "UFUEAA".

        Sends in batches of size determined by 'batch_size' passed to
        constructor. All requests to BSD API are attempted concurrently.

        Before sending, populates rows in triggered_email_sends with
        placeholder mailing_recipient_id; after sending, updates these rows
        with the deferred_task_id that BSD API responds with.

        get_deferred_results.py is responsible for then fetching the final BSD API result.
        """

        print(f"Sending triggered emails {mailing_name} ({mailing_id}) for ds {ds}.")
        print(
            f'(Override email: {self.override_email}; cc_emails: {", ".join(self.cc_emails)})'
        )
        print(f"(Connection concurrency limit: {CONNECTION_CONCURRENCY_LIMIT})")

        if count_first:
            count_to_send = await super()._get_schedulings_count(ds, mailing_name)
        else:
            count_to_send = "(not computed)"
        print(f"Will send {count_to_send} emails in batches of {self.batch_size}.")

        loop = asyncio.get_event_loop()
        connector = aiohttp.TCPConnector(limit=CONNECTION_CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
            sent_in_last_batch = 1
            sent_total = 0
            batch_i = 0
            while sent_in_last_batch > 0:
                sent_in_last_batch = await self._send_batch(
                    session,
                    ds,
                    mailing_name,
                    mailing_id,
                    batch_i,
                    sent_total,
                    count_to_send,
                )
                sent_total += sent_in_last_batch
                batch_i += 1
                if self.limit_per_mailing and sent_total >= self.limit_per_mailing:
                    print(f"Hit limit of {self.limit_per_mailing} sendings")
                    break

        print(f"Finished sending {sent_total} mails.")

    async def _send_batch(
        self,
        session,
        ds,
        mailing_name,
        mailing_id,
        batch_i,
        info_sent_total,
        info_count_to_send,
    ):
        # Print debug info only for the first batch.
        debug = batch_i == 0

        t0 = time.time()
        schedulings = await super()._get_schedulings(ds, mailing_name, debug)

        if not schedulings.empty and self.email_whitelist:
            schedulings = schedulings[schedulings["email"].isin(self.email_whitelist)]

        if len(schedulings) == 0:
            print("No schedulings to send.")
            return 0

        t1 = time.time()
        time_get_schedulings = t1 - t0

        await super()._insert_placeholder_sends(
            schedulings, mailing_id, ds_today(), debug
        )

        t2 = time.time()
        time_insert_placeholder_sends = t2 - t1

        schedulings_with_send_status = [
            {"scheduling": scheduling}
            for scheduling in schedulings.itertuples(index=False)
        ]

        await self._send_schedulings_async(
            session, schedulings_with_send_status, mailing_id, debug
        )

        t3 = time.time()
        time_send_schedulings = t3 - t2

        await self._process_triggered_email_api_responses_async(
            session, schedulings_with_send_status, debug
        )

        t4 = time.time()
        time_process_triggered_email_api_responses = t4 - t3

        await super()._replace_placeholder_sends(
            ds, mailing_name, mailing_id, schedulings_with_send_status, debug
        )

        t5 = time.time()
        time_replace_placeholder_sends = t5 - t4

        time_total = t5 - t0
        num_sent = len(schedulings_with_send_status)

        print(
            f"Sent {info_sent_total + num_sent}/{info_count_to_send} in {time_total}. "
            f"Timings: get_schedulings {time_get_schedulings:.2f}; "
            f"insert_placeholder_sends {time_insert_placeholder_sends:.2f}; "
            f"send_schedulings {time_send_schedulings:.2f}; "
            f"process_triggered_email_api_responses {time_process_triggered_email_api_responses:.2f}; "
            f"replace_placeholder_sends {time_replace_placeholder_sends:.2f}; "
        )

        return num_sent

    async def _send_schedulings_async(
        self, session, schedulings_with_send_status, mailing_id, debug
    ):
        if CONNECTION_CONCURRENCY_LIMIT == 1:
            results = []
            for i, scheduling_with_send_status in enumerate(
                schedulings_with_send_status
            ):
                try:
                    result = await self._send_scheduling_async(
                        session,
                        scheduling_with_send_status["scheduling"],
                        mailing_id,
                        debug,
                        i,
                    )
                except Exception as e:
                    result = e
                results.append(result)
                await asyncio.sleep(SLEEP_BETWEEN_CONCURRENT_MAILS)
        else:
            results = await asyncio.gather(
                *[
                    self._send_scheduling_async(
                        session,
                        scheduling_with_send_status["scheduling"],
                        mailing_id,
                        debug,
                        i,
                    )
                    for i, scheduling_with_send_status in enumerate(
                        schedulings_with_send_status
                    )
                ],
                return_exceptions=True,
            )
        for scheduling_with_send_status, send_status in zip(
            schedulings_with_send_status, results
        ):
            if isinstance(send_status, Exception):
                print("Got exception from send_scheduling_async")
                print(send_status)
            else:
                scheduling_with_send_status.update(send_status)

    async def _send_scheduling_async(self, session, scheduling, mailing_id, debug, i):
        payload = scheduling._asdict()
        super()._massage_payload(payload)

        if self.override_email:
            destination_email = self.override_email[
                i % len(self.override_email)
            ].replace(OVERRIDE_EMAIL_N_PLACEHOLDER, str(i))
            payload["subject_prefix"] = f"[orig {scheduling.email}] "

            # In case of override_email, make sure payload matches.
            payload["email"] = destination_email
        else:
            destination_email = scheduling.email

        payload_str = json.dumps(payload)
        payload_str_urlencoded = urllib.parse.quote(payload_str)
        base_params = {
            "mailing_id": mailing_id,
            "email": destination_email,
            "email_opt_in": 0,
            "trigger_values": payload_str_urlencoded,
        }

        params = base_params.copy()

        if debug and self._should_sample_log():
            print(f"Making triggered email request with params {params}")

        status, body = await self.bsd_api.doRequest(
            session, "/mailer/send_triggered_email", params, "POST"
        )

        for cc_email in self.cc_emails:
            cc_payload = payload.copy()
            cc_payload["subject_prefix"] = f"[orig {scheduling.email}]"
            cc_payload_str = json.dumps(cc_payload)
            cc_payload_str_urlencoded = urllib.parse.quote(cc_payload_str)

            cc_params = base_params.copy()
            cc_params["email"] = cc_email
            cc_params["trigger_values"] = cc_payload_str_urlencoded
            if CONNECTION_CONCURRENCY_LIMIT == 1:
                await asyncio.sleep(SLEEP_BETWEEN_CONCURRENT_MAILS)
            cc_status, _ = await self.bsd_api.doRequest(
                session, "/mailer/send_triggered_email", cc_params, "POST"
            )
            if cc_status != 202:
                print(f"Failed to send to {cc_email}")

        return {"resp_status": status, "resp_body": body, "sent_ds": ds_today()}

    def _should_sample_log(self):
        return random.random() < 0.01

    async def _process_triggered_email_api_responses_async(
        self, session, schedulings_with_send_status, debug
    ):
        results = await asyncio.gather(
            *[
                self._process_triggered_email_api_response_async(
                    session,
                    scheduling_with_send_status["resp_status"],
                    scheduling_with_send_status["resp_body"],
                    debug,
                )
                for scheduling_with_send_status in schedulings_with_send_status
            ],
            return_exceptions=True,
        )
        for scheduling_with_send_status, send_status in zip(
            schedulings_with_send_status, results
        ):
            scheduling_with_send_status.update(send_status)

    async def _process_triggered_email_api_response_async(
        self, session, status, body, debug
    ):
        # This method used to hit the BSD deferred results API; now we do this
        # in a later program.

        if "This e-mail address is unsubscribed and cannot be delivered to" in body:
            mailing_recipient_id = UNSUB_MAILING_RECIPIENT_ID
            mailing_send_id = UNSUB_MAILING_SEND_ID
            deferred_task_id = None
        elif "Must be a valid e-mail address" in body:
            mailing_recipient_id = INVAL_MAILING_RECIPIENT_ID
            mailing_send_id = INVAL_MAILING_SEND_ID
            deferred_task_id = None
        elif status == 202 and body:
            mailing_recipient_id = DEFERRED_MAILING_RECIPIENT_ID
            mailing_send_id = DEFERRED_MAILING_SEND_ID
            deferred_task_id = json.loads(body)["deferred_task_id"]
        elif status == 503:
            print("Got a 503 response:", body)
            mailing_recipient_id = ERROR_MAILING_RECIPIENT_ID
            mailing_send_id = ERROR_MAILING_SEND_ID
            deferred_task_id = None
        else:
            result = None
            try:
                result = json.loads(body)
            except json.decoder.JSONDecodeError:
                print("Got non-JSON response:", body)
                print("Status was", status)

            if result:
                mailing_recipient_id = result["mailing_recipient_id"]
                mailing_send_id = result["mailing_send_id"]
                deferred_task_id = None
            else:
                mailing_recipient_id = ERROR_MAILING_RECIPIENT_ID
                mailing_send_id = ERROR_MAILING_SEND_ID
                deferred_task_id = None

        return {
            "mailing_recipient_id": mailing_recipient_id,
            "mailing_send_id": mailing_send_id,
            "deferred_task_id": deferred_task_id,
        }
