# Sends scheduled triggered emails for a given mailing for a given day by
# uploading a cons group and personalization dataset to BSD.
#
# This is especially useful in a BSD "recurring mailing" context -- if a
# recurring mailing is set to send to a cons group, with a template using that
# personalization dataset, you can totally automate a large daily send.

import asyncio
import json
import random
import time
import urllib
import zipfile

import aiohttp
import pandas as pd
from unidecode import unidecode

from .utils import ds_today
from .api import BsdApi
from .send_base import (
    ERROR_MAILING_RECIPIENT_ID,
    ERROR_MAILING_SEND_ID,
    SendBase,
)

RECURRING_SEND_PLACEHOLDER_MAILING_RECIPIENT_ID = "PLACE_RECUR"
RECURRING_SEND_PLACEHOLDER_MAILING_SEND_ID = "PLACE_RECUR"
RECURRING_MAILING_ID = "RECUR"

# BSD has an arbitrary length limit on personalization dataset name
PERSONALIZATION_DATASET_SLUG_MAX_LENGTH = 30


class BsdTriggeredEmailSendRecurring(SendBase):
    DELETE_PLACEHOLDER_SENDS_SQL = """
DELETE FROM "{schema}"."triggered_email_sends"
 WHERE ds={ds}
   AND mailing_name={mailing_name}
   AND mailing_recipient_id={mailing_recipient_id}
   AND mailing_send_id={mailing_send_id}
;
"""

    def __init__(
        self,
        civis,
        bsd_host,
        bsd_api_id,
        bsd_secret,
        schema,
        cc_emails,
        limit_per_mailing,
        batch_size,
    ):
        super().__init__(civis, schema, limit_per_mailing, batch_size)
        self.bsd_api = BsdApi(bsd_api_id, bsd_secret, bsd_host, 80, 443)
        self.cc_emails = cc_emails

        if self.limit_per_mailing and self.batch_size > self.limit_per_mailing:
            print(
                f"Batch size {self.batch_size} larger than limit {self.limit_per_mailing}; setting batch size to {self.limit_per_mailing}"
            )
            self.batch_size = self.limit_per_mailing

    async def send(
        self,
        ds_list,
        mailing_name,
        cons_group_id,
        personalization_dataset_columns,
        personalization_dataset_name_prefix,
        count_first=True,
    ):
        """Sends all scheduled mails for list of dates 'ds_list' for mailing 'mailing_name'.

        Assumes recurring mailing is set up to send daily to all cons in given cons_group_id.

        For each column in list 'personalization_dataset_columns', we'll upload
        a personalization dataset with slug '{personalization_dataset_name_prefix}_{personalization_dataset_column}'
        which has two columns -- cons ID and the personalized value. So for an
        email payload with 6 columns, we'll create 6 personalization data sets.
        We create one personalization dataset per column, instead of one
        personalization dataset with all columns, because BSD personalization
        dataset upload API is not reliable for large files, so multiple smaller
        files is more reliable :-/
        """

        print(f"Sending {mailing_name} for ds {ds_list}.")

        personalization_dataset_map = None
        personalization_dataset = None
        cons_ids = []

        sent_total = 0
        batch_i = 0

        cons_id_identity_column_name = f"cons_id_{mailing_name}"

        personalization_dataset_columns_with_identity = (
            personalization_dataset_columns.copy()
        )
        if cons_id_identity_column_name not in personalization_dataset_columns:
            personalization_dataset_columns_with_identity.insert(
                0, cons_id_identity_column_name
            )

        for ds in ds_list:
            await self._delete_all_sends_for_ds(
                ds,
                mailing_name,
                RECURRING_MAILING_ID,
                mailing_recipient_id=RECURRING_SEND_PLACEHOLDER_MAILING_RECIPIENT_ID,
                mailing_send_id=RECURRING_SEND_PLACEHOLDER_MAILING_SEND_ID,
                debug=True,
            )

            if count_first:
                count_to_send = await self._get_schedulings_count(ds, mailing_name)
            else:
                count_to_send = "(not computed)"
            print(
                f"Will send {count_to_send} emails for {ds} in batches of {self.batch_size}."
            )

            sent_in_last_batch = 1
            sent_total_in_ds = 0
            while sent_in_last_batch > 0:
                # We'll prepare two dataframes in place:
                # - personalization_dataset_map_batch
                # - personalization_dataset_batch
                # These will be concatenated all together across all batches.
                personalization_dataset_batch = await self._get_batch(
                    ds, mailing_name, batch_i, sent_total_in_ds, count_to_send
                )

                if personalization_dataset_batch.empty:
                    break

                # Maintain running list of all cons_ids, from which we'll later create a cons_group.
                cons_ids.extend(personalization_dataset_batch["cons_id"].tolist())

                # The dataset_map is two columns, 'cons_id' and 'cons_id_identity',
                # with identical rows. This tells BSD to map 'cons_id' to
                # 'cons_id_identity', and then to look up the 'cons_id_identity' in
                # the personalization dataset. The result is we can personalize
                # each send to the specific cons_id.
                personalization_dataset_map_batch = personalization_dataset_batch[
                    ["cons_id"]
                ]
                personalization_dataset_map_batch.insert(
                    1,
                    cons_id_identity_column_name,
                    personalization_dataset_map_batch["cons_id"],
                )

                # The personalization dataset itself is the same as the dataframe
                # returned by _get_batch(), but with cons_id renamed to
                # cons_id_identity and moved to be the first column.
                #
                # This is because BSD uses the first column as the index into the
                # dataset, and the index should be the cons_id_identity.
                personalization_dataset_batch.insert(
                    0,
                    cons_id_identity_column_name,
                    personalization_dataset_batch.pop("cons_id"),
                )

                personalization_dataset_batch.drop(
                    personalization_dataset_batch.columns.difference(
                        personalization_dataset_columns_with_identity
                    ),
                    axis=1,
                    inplace=True,
                )

                # BSD cannot handle emoji in personalization data sets : (
                # It's unclear whether it can handle accents or other Unicode.
                # So we pass it through this function which does its best
                # effort to map unicode to ascii.
                personalization_dataset_batch = personalization_dataset_batch.applymap(
                    self._ensure_only_ascii_strings
                )

                if personalization_dataset is None:
                    personalization_dataset = personalization_dataset_batch
                    personalization_dataset_map = personalization_dataset_map_batch
                else:
                    personalization_dataset = pd.concat(
                        [personalization_dataset, personalization_dataset_batch],
                        ignore_index=True,
                    )
                    personalization_dataset_map = pd.concat(
                        [
                            personalization_dataset_map,
                            personalization_dataset_map_batch,
                        ],
                        ignore_index=True,
                    )

                sent_in_last_batch = len(personalization_dataset_batch)
                sent_total += sent_in_last_batch
                sent_total_in_ds += sent_in_last_batch
                if self.limit_per_mailing and sent_total >= self.limit_per_mailing:
                    print(f"Hit limit of {self.limit_per_mailing} sendings")
                    break

                batch_i += 1

        # Ensure we don't upload the same cons twice. This can happen in
        # obscure edge cases where an email address is shared by multiple
        # constituents. This seems to result either from a race condition in
        # BSD or email addresses that are different by '.' or other cosmetic
        # differences.
        if personalization_dataset is not None:
            personalization_dataset.drop_duplicates(
                cons_id_identity_column_name, inplace=True
            )

        print("Will upload personalization_dataset_map:")
        print(personalization_dataset_map)
        print("Will upload personalization_dataset:")
        print(personalization_dataset)

        async with aiohttp.ClientSession() as session:
            await self._upload_cons_group(session, cons_group_id, cons_ids)
            await self._upload_personalization_dataset_map(
                session, personalization_dataset_map
            )
            await self._upload_personalization_dataset(
                session,
                personalization_dataset,
                personalization_dataset_columns,
                personalization_dataset_name_prefix,
                cons_id_identity_column_name,
            )

        print(
            f"Finished creating cons group, personalization dataset map, and personalization dataset for {sent_total} mails."
        )

        # TODO(Jason Katz-Brown): Replace PLACE_RECUR send rows with different state.

    async def _get_batch(
        self, ds, mailing_name, batch_i, info_sent_total, info_count_to_send
    ):
        # Print debug info only for the first batch.
        debug = batch_i == 0

        t0 = time.time()
        schedulings = await self._get_schedulings(ds, mailing_name, debug)

        if len(schedulings) == 0:
            print("No schedulings to send.")
            return pd.DataFrame()

        t1 = time.time()
        time_get_schedulings = t1 - t0

        await self._insert_placeholder_sends(
            schedulings,
            RECURRING_MAILING_ID,
            ds_today(),
            debug,
            placeholder_mailing_recipient_id=RECURRING_SEND_PLACEHOLDER_MAILING_RECIPIENT_ID,
            placeholder_mailing_send_id=RECURRING_SEND_PLACEHOLDER_MAILING_SEND_ID,
        )

        t2 = time.time()
        time_insert_placeholder_sends = t2 - t1

        time_total = t2 - t0

        print(
            f"Got {info_sent_total + len(schedulings)}/{info_count_to_send} in {time_total}. "
            f"Timings: get_schedulings {time_get_schedulings:.2f}; "
            f"insert_placeholder_sends {time_insert_placeholder_sends:.2f}; "
        )

        return schedulings

    async def _upload_cons_group(self, session, cons_group_id, cons_ids):
        """Uploads cons group of email recipients"""
        await self.bsd_api.setConsIdsForGroup(session, cons_group_id, cons_ids)

    async def _upload_personalization_dataset_map(
        self, session, personalization_dataset_map
    ):
        """Uploads personalization dataset map of cons ID to cons ID.

        This is a necessary intermediary mapping; we map cons ID to cons ID but
        with a different column name (stored in cons_id_identity_column_name
        variable elsewhere). Then this other column is mapped to the actual
        personalization values in the personalization_data_map.
        """
        if personalization_dataset_map is not None:
            personalization_dataset_map_csv = personalization_dataset_map.to_csv(
                index=False
            )
            url_encoded_csv = urllib.parse.quote(personalization_dataset_map_csv)
            dataset_map_upload_body = f"csv_data={url_encoded_csv}"
            status, body = await self.bsd_api.doRequest(
                session, "/cons/upload_dataset_map", {}, "POST", dataset_map_upload_body
            )
            await self.bsd_api.wait_for_deferred_result(
                session, status, body, "personalization dataset map upload"
            )
        else:
            print(
                "No people to personalize to; skipping uploading personalization dataset and dataset_map."
            )

    async def _upload_personalization_dataset(
        self,
        session,
        personalization_dataset,
        personalization_dataset_columns,
        personalization_dataset_name_prefix,
        cons_id_identity_column_name,
    ):
        if personalization_dataset is not None:
            for col in personalization_dataset_columns:
                personalization_dataset_for_col = personalization_dataset[
                    [cons_id_identity_column_name, col]
                ]

                filename = f"personalization_dataset_{col}.csv"
                csv_filename = f"/tmp/{filename}"
                # Write a copy also to disk for easy debugging.
                personalization_dataset_for_col.to_csv(csv_filename, index=False)

                zip_filename = f"/tmp/{filename}.zip"
                zipfile.ZipFile(
                    zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED
                ).write(csv_filename, filename)

                personalization_dataset_csv_for_col = personalization_dataset_for_col.to_csv(
                    index=False
                )

                slug = f"{personalization_dataset_name_prefix}_{col}"
                if len(slug) > PERSONALIZATION_DATASET_SLUG_MAX_LENGTH:
                    raise ValueError(
                        f"All personalization dataset names must be {PERSONALIZATION_DATASET_SLUG_MAX_LENGTH} characters or shorter. {slug} is too long."
                    )

                dataset_params = {
                    "slug": slug,
                    "map_type": cons_id_identity_column_name,
                    "zipped": 1,
                }
                with open(zip_filename, "rb") as f:
                    status, body = await self.bsd_api.doRequest(
                        session, "/cons/upload_dataset", dataset_params, "POST", f
                    )
                await self.bsd_api.wait_for_deferred_result(
                    session, status, body, f"personalization dataset {col} upload"
                )

    def _ensure_only_ascii_strings(self, v):
        if isinstance(v, str):
            return unidecode(v)
        return v
