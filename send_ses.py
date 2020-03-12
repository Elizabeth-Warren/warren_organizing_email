# Sends scheduled triggered emails for a given mailing for a given day by
# making requests to Amazon Simple Email Service.
#
# Pulls in email content templates stored in Contentful using Contentful API.

import asyncio
import json
import random
import time

import aiohttp
import boto3
import botocore.exceptions
import contentful
import pandas as pd

from .utils import ds_today, sql_quote
from .send_base import (
    ERROR_MAILING_RECIPIENT_ID,
    ERROR_MAILING_SEND_ID,
    SendBase,
)

SES_REGION_NAME = "us-east-1"

CONTENTFUL_CONTENT_TYPE_MAILING = "mailing"

MAILING_ID_SES = "ses"

OVERRIDE_EMAIL_N_PLACEHOLDER = "{{n}}"


class SesTriggeredEmailSendBatch:
    def __init__(
        self,
        civis,
        contentful_space_id,
        contentful_access_token,
        schema,
        override_email,
        cc_emails,
        limit_per_mailing,
        batch_size,
    ):
        super().__init__(civis, schema, limit_per_mailing, batch_size)
        self.cc_emails = cc_emails

        self.contentful_client = contentful.Client(
            contentful_space_id, contentful_access_token
        )
        self.ses = boto3.client("ses", region_name=SES_REGION_NAME)

        self.override_email = override_email
        if self.override_email:
            self.override_email = self.override_email.split(",")

        if self.limit_per_mailing and self.batch_size > self.limit_per_mailing:
            print(
                f"Batch size {self.batch_size} larger than limit {self.limit_per_mailing}; setting batch size to {self.limit_per_mailing}"
            )
            self.batch_size = self.limit_per_mailing

    async def send(self, ds, mailing_name):
        """Sends all scheduled mails for date 'ds' for mailing 'mailing_name'.

        Sends in batches of size determined by 'batch_size' passed to
        constructor.

        Before sending, populates rows in triggered_email_sends with
        placeholder mailing_recipient_id; after sending, updates these rows
        with the mailing ID returned by SES.
        """

        mailing_entry = self._update_or_create_ses_template(mailing_name)

        print(f"Sending triggered emails {mailing_name} for ds {ds}.")
        print(
            f'(Override email: {self.override_email}; cc_emails: {", ".join(self.cc_emails)})'
        )

        count_to_send = await super()._get_schedulings_count(ds, mailing_name)
        print(f"Will send {count_to_send} emails in batches of {self.batch_size}.")

        # TODO(Jason Katz-Brown): This doesn't need to be asyncio.
        loop = asyncio.get_event_loop()
        connector = aiohttp.TCPConnector(limit=100)
        async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
            sent_in_last_batch = 1
            sent_total = 0
            batch_i = 0
            while sent_in_last_batch > 0:
                sent_in_last_batch = await self._send_batch(
                    session,
                    ds,
                    mailing_name,
                    mailing_entry,
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

    def _update_or_create_ses_template(self, mailing_name):
        """Creates or updates SES template from mailing and boilerplate in Contentful.

        Returns Mailing entry from Contentful.
        """
        mailing_entries = self.contentful_client.entries(
            {
                "content_type": CONTENTFUL_CONTENT_TYPE_MAILING,
                "fields.name[match]": mailing_name,
            }
        )
        if len(mailing_entries) != 1:
            raise ValueError(
                f"Could not precisely find mailing in contentful: {mailing_name}"
            )
        mailing_entry = mailing_entries[0]

        boilerplate = mailing_entry.boilerplate
        text_part = (
            boilerplate.fields().get("text_header", "")
            + mailing_entry.text_template
            + boilerplate.fields().get("text_footer", "")
        )
        html_part = (
            boilerplate.fields().get("html_header", "")
            + mailing_entry.html_template
            + boilerplate.fields().get("html_footer", "")
        )

        ses_template_spec = {
            "TemplateName": mailing_name,
            "SubjectPart": mailing_entry.subject,
            "TextPart": text_part,
            "HtmlPart": html_part,
        }
        try:
            self.ses.update_template(Template=ses_template_spec)
        except botocore.exceptions.TemplateDoesNotExistException:
            self.ses.create_template(Template=ses_template_spec)

        return mailing_entry

    async def _send_batch(
        self,
        session,
        ds,
        mailing_name,
        mailing_entry,
        batch_i,
        info_sent_total,
        info_count_to_send,
    ):
        # Print debug info only for the first batch.
        debug = batch_i == 0

        t0 = time.time()
        schedulings = await super()._get_schedulings(ds, mailing_name, debug)

        if len(schedulings) == 0:
            print("No schedulings to send.")
            return 0

        t1 = time.time()
        time_get_schedulings = t1 - t0

        await super()._insert_placeholder_sends(
            schedulings, MAILING_ID_SES, ds_today(), debug
        )

        t2 = time.time()
        time_insert_placeholder_sends = t2 - t1

        schedulings_with_send_status = [
            {"scheduling": scheduling} for scheduling in schedulings.itertuples()
        ]

        await self._send_schedulings_async(
            session, schedulings_with_send_status, mailing_entry, debug
        )

        t3 = time.time()
        time_send_schedulings = t3 - t2

        await self._process_triggered_email_api_responses_async(
            session, schedulings_with_send_status, debug
        )

        t4 = time.time()
        time_process_triggered_email_api_responses = t4 - t3

        await super()._replace_placeholder_sends(
            ds, mailing_name, MAILING_ID_SES, schedulings_with_send_status, debug
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
        self, session, schedulings_with_send_status, mailing_entry, debug
    ):
        results = await asyncio.gather(
            *[
                self._send_scheduling_async(
                    session,
                    scheduling_with_send_status["scheduling"],
                    mailing_entry,
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

    async def _send_scheduling_async(
        self, session, scheduling, mailing_entry, debug, i
    ):
        payload = scheduling._asdict()
        super()._massage_payload(payload)

        if self.override_email:
            destination_email = self.override_email[
                i % len(self.override_email)
            ].replace(OVERRIDE_EMAIL_N_PLACEHOLDER, str(i))
            payload[
                "subject_prefix"
            ] = f"[ override {destination_email} orig {scheduling.email} ]"

            # In case of override_email, make sure payload matches.
            payload["email"] = destination_email
        else:
            destination_email = scheduling.email

        request = {
            "Source": mailing_entry.from_email,
            "Destination": {
                "ToAddresses": [destination_email],
                "BccAddresses": mailing_entry.fields().get("bcc_emails", []),
            },
            "ReplyToAddresses": [mailing_entry.reply_to_email],
            "Template": scheduling.mailing_name,
            "TemplateData": json.dumps(payload),
            "ConfigurationSetName": mailing_entry.configuration_set_name,
        }
        response = self.ses.send_templated_email(**request)

        if debug and self._should_sample_log():
            print("Making SES send_templated_email request", request)
            print("Got response from SES", response)

        return {
            "response": response,
            "configuration_set_name": mailing_entry.configuration_set_name,
            "sent_ds": ds_today(),
        }

    def _should_sample_log(self):
        return random.random() < 0.01

    async def _process_triggered_email_api_responses_async(
        self, session, schedulings_with_send_status, debug
    ):
        results = await asyncio.gather(
            *[
                self._process_triggered_email_api_response_async(
                    session, scheduling_with_send_status, debug
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
        self, session, scheduling_with_send_status, debug
    ):
        # Typical SES response: {'MessageId': '0100016d8564c23c-92d73e00-5a27-49d2-a983-946f1512e4e7-000000', 'ResponseMetadata': {'RequestId': '8bbd5a2c-e1aa-4ed1-9ea2-b7592c16e1e3', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': '8bbd5a2c-e1aa-4ed1-9ea2-b7592c16e1e3', 'content-type': 'text/xml', 'content-length': '362', 'date': 'Tue, 01 Oct 2019 03:37:15 GMT'}, 'RetryAttempts': 0}}
        response = scheduling_with_send_status.get("response")
        if response:
            return {
                "mailing_recipient_id": response["MessageId"],
                "mailing_send_id": scheduling_with_send_status[
                    "configuration_set_name"
                ],
                "deferred_task_id": None,
            }
        else:
            return {
                "mailing_recipient_id": ERROR_MAILING_RECIPIENT_ID,
                "mailing_send_id": ERROR_MAILING_SEND_ID,
                "deferred_task_id": None,
            }
