# Prepares spreadsheet summarizing email schedulings.
#
# summarize_all_time() prepares two tabs:
#   "Summary All-Time" tab: Aggregates counts by day and by 'summary_all_time_group_by_fields', a comma-separated list of payload fields. For example, 
#     'event_state, event_url, event_title, event_type, event_start_timestamp_local'
#
#   "Summary By Week" tab: Total scheduling count by week
#
# forecast() prepares one tab, all schedulings on date 'ds', one row per
# recipient on date 'ds', up to 'limit' recipients.
#
# Other inputs:
#   'mailing_name', e.g. 'event_invite'
#   'output_sheet', e.g. 'https://docs.google.com/spreadsheets/d/1KcZIW6piCZ60GR68KTN_UJB5wpfIh8Idc2b2E-7enFs'

import asyncio
import datetime

import pandas as pd


class BsdTriggeredEmailForecast:
    FROM_SCHEDULINGS_JOIN_PAYLOADS = """
  FROM "{schema}"."triggered_email_schedulings"
  JOIN "{schema}"."triggered_email_payloads_{mailing_name}"
    ON "triggered_email_payloads_{mailing_name}".ds = "triggered_email_schedulings".ds
   AND (("triggered_email_payloads_{mailing_name}".cons_id IS NULL AND "triggered_email_schedulings".cons_id IS NULL)
        OR "triggered_email_payloads_{mailing_name}".cons_id = "triggered_email_schedulings".cons_id)
   AND "triggered_email_payloads_{mailing_name}".email = "triggered_email_schedulings".email
   AND (("triggered_email_payloads_{mailing_name}".secondary_id IS NULL AND "triggered_email_schedulings".secondary_id IS NULL)
        OR "triggered_email_payloads_{mailing_name}".secondary_id = "triggered_email_schedulings".secondary_id)
"""

    GET_SCHEDULINGS_SQL = """
SELECT
  "triggered_email_schedulings".email
, {output_fields}

FROM_AND_JOIN_GOES_HERE

 WHERE "triggered_email_schedulings".ds = '{ds}'
   AND "triggered_email_schedulings".mailing_name = '{mailing_name}'
 ORDER BY "triggered_email_schedulings".email
     , "triggered_email_schedulings".secondary_id
     , "triggered_email_schedulings".scheduled_at
 LIMIT {limit}
;
""".replace(
        "FROM_AND_JOIN_GOES_HERE", FROM_SCHEDULINGS_JOIN_PAYLOADS
    )

    GET_SUMMARY_ALL_TIME_SQL = """
SELECT
  "triggered_email_schedulings".ds
, {summary_all_time_group_by_fields}
, COUNT(*) AS cons_count

FROM_AND_JOIN_GOES_HERE

 WHERE "triggered_email_schedulings".mailing_name = '{mailing_name}'
 GROUP BY "triggered_email_schedulings".ds, {summary_all_time_group_by_fields}
 ORDER BY 1 DESC, cons_count DESC
;
""".replace(
        "FROM_AND_JOIN_GOES_HERE", FROM_SCHEDULINGS_JOIN_PAYLOADS
    )

    GET_SUMMARY_BY_WEEK_SQL = """
SELECT
  DATE_TRUNC('w', "triggered_email_schedulings".ds) AS week_begin
, COUNT(*) AS cons_count

FROM_AND_JOIN_GOES_HERE

 WHERE "triggered_email_schedulings".mailing_name = '{mailing_name}'
 GROUP BY DATE_TRUNC('w', "triggered_email_schedulings".ds)
 ORDER BY 1 DESC
;
""".replace(
        "FROM_AND_JOIN_GOES_HERE", FROM_SCHEDULINGS_JOIN_PAYLOADS
    )

    TAB_NAME_SUMMARY_ALL_TIME = "Summary All-Time"
    TAB_NAME_SUMMARY_BY_WEEK = "Summary By Week"

    def __init__(self, civis, schema, caliban):
        self.civis = civis
        self.schema = schema
        self.caliban = caliban

    def forecast(self, ds, mailing_name, output_sheet, output_fields, tab_name, limit):
        schedulings = self.get_schedulings(ds, mailing_name, output_fields, limit)
        if schedulings.empty:
            return

        self.caliban.export_to_worksheets(output_sheet, tab_name, schedulings)

    def summarize_all_time(
        self, mailing_name, output_sheet, summary_all_time_group_by_fields
    ):
        if not summary_all_time_group_by_fields:
            return

        summary = self.get_summary_all_time(
            mailing_name, summary_all_time_group_by_fields
        )
        self.caliban.export_to_worksheets(
            output_sheet, self.TAB_NAME_SUMMARY_ALL_TIME, summary
        )

        summary_by_week = self.get_summary_by_week(mailing_name)
        self.caliban.export_to_worksheets(
            output_sheet, self.TAB_NAME_SUMMARY_BY_WEEK, summary_by_week
        )

    def get_schedulings(self, ds, mailing_name, output_fields, limit):
        query = self.GET_SCHEDULINGS_SQL.format(
            ds=ds,
            mailing_name=mailing_name,
            schema=self.schema,
            output_fields=output_fields,
            limit=limit,
        )
        print("get_schedulings query:")
        print(query)
        df = asyncio.run(self.civis.read_civis_sql(query))
        print(f"Got {len(df)} schedulings for {mailing_name} on {ds}:")
        print(df)

        return df

    def get_summary_all_time(self, mailing_name, summary_all_time_group_by_fields):
        query = self.GET_SUMMARY_ALL_TIME_SQL.format(
            mailing_name=mailing_name,
            schema=self.schema,
            summary_all_time_group_by_fields=summary_all_time_group_by_fields,
        )
        print("get_summary_all_time query:")
        print(query)
        df = asyncio.run(self.civis.read_civis_sql(query))
        print(f"Got summary all-time for {mailing_name}:")
        print(df)

        return df

    def get_summary_by_week(self, mailing_name):
        query = self.GET_SUMMARY_BY_WEEK_SQL.format(
            mailing_name=mailing_name, schema=self.schema
        )
        print("get_summary_by_week query:")
        print(query)
        df = asyncio.run(self.civis.read_civis_sql(query))
        print(f"Got summary by week for {mailing_name}:")
        print(df)

        return df
