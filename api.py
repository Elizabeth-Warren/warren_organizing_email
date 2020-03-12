# Client for BSD API that supports concurrent requests.

import asyncio
import pprint
import re
import urllib
import xml.etree.ElementTree

import aiohttp
import xmltodict
from bsdapi.RequestGenerator import RequestGenerator


class BsdAPIException(RuntimeError):
    pass


class BsdApi:
    """A reworking of bsdapi module's BsdApi that uses aiohttp."""

    def __init__(self, apiId, apiSecret, apiHost, apiPort=80, apiSecurePort=443):
        self.apiId = apiId
        self.apiSecret = apiSecret
        self.apiHost = apiHost
        self.apiPort = apiPort
        self.apiSecurePort = apiSecurePort

    async def getDeferredResults(self, session, deferred_id):
        query = {"deferred_id": deferred_id}
        url_secure = self._generateRequest("/get_deferred_results", query)
        return await self._makeGETRequest(session, url_secure)

    WAIT_BETWEEN_DEFERRED_RESULT_POLLING = 1

    async def wait_for_deferred_result(
        self, session, status, body, whats_this_for, print_res=True
    ):
        deferred_task_id = body
        while status == 202 or "Rate limit exceeded" in body:
            await asyncio.sleep(self.WAIT_BETWEEN_DEFERRED_RESULT_POLLING)
            status, body = await self.getDeferredResults(session, deferred_task_id)

        if print_res:
            print(f"Deferred result for {whats_this_for}:", status, body)

        return status, body

    async def getTriggeredSend(self, session, mailing_recipient_id):
        query = {"mailing_recipient_id": mailing_recipient_id}
        url_secure = self._generateRequest("/mailer/get_triggered_send", query)
        return await self._makeGETRequest(session, url_secure)

    async def getConsGroupByName(self, session, name):
        """Returns cons group ID with given name if exists, None otherwise."""
        query = {"name": name}
        status, body = await self.doRequest(
            session, "/cons_group/get_constituent_group_by_name", query
        )
        d = xmltodict.parse(body, attr_prefix="", cdata_key="value")
        api_result = d["api"]
        if api_result:
            return api_result["cons_group"]["id"]

    CREATE_CONS_GROUP_EXTANT_GROUP_RESPONSE_RE = re.compile(
        "already exists with id: (\d+)"
    )

    async def createConsGroup(self, session, name):
        """Create cons group with given name. If one already exists, returns its id."""
        top_element = xml.etree.ElementTree.Element("api")
        cons_group_element = xml.etree.ElementTree.SubElement(top_element, "cons_group")
        xml.etree.ElementTree.SubElement(cons_group_element, "name").text = name
        cons_group_xml = (
            b'<?xml version="1.0" encoding="utf-8"?>'
            + xml.etree.ElementTree.tostring(top_element)
        )
        status, body = await self.doRequest(
            session, "/cons_group/add_constituent_groups", {}, "POST", cons_group_xml
        )

        extant_group_match = self.CREATE_CONS_GROUP_EXTANT_GROUP_RESPONSE_RE.search(
            body
        )
        if extant_group_match:
            return int(extant_group_match.group(1))

        d = xmltodict.parse(body, attr_prefix="", cdata_key="value")
        api_result = d["api"]
        return api_result["cons_group"]["id"]

    async def setConsIdsForGroup(self, session, cons_group_id, cons_ids):
        query = {"cons_group_id": cons_group_id}
        url_encoded_cons_ids = "cons_ids=" + urllib.parse.quote(",".join(cons_ids))
        status, body = await self.doRequest(
            session,
            "/cons_group/set_cons_ids_for_group",
            query,
            "POST",
            url_encoded_cons_ids,
        )
        return await self.wait_for_deferred_result(
            session, status, body, "cons group upload"
        )

    async def getConsIdsForGroup(self, session, cons_group_id):
        query = {"cons_group_id": cons_group_id}
        status, body = await self.doRequest(
            session, "/cons_group/get_cons_ids_for_group", query, "GET"
        )

        status, body = await self.wait_for_deferred_result(
            session, status, body, "cons group get members", print_res=False
        )

        # We can't use check_for_error because it's not an XML response
        if status != 200:
            raise BsdAPIException("Status code: {}. Message: {}".format(status, body))

        trimmed_body = body.strip()
        if len(trimmed_body) == 0:
            return []
        return trimmed_body.split("\n")

    async def cons_upsertConstituentData(self, session, xml_data):
        status, body = await self.doRequest(
            session, "/cons/upsert_constituent_data", request_type="POST", body=xml_data
        )
        return await self.wait_for_deferred_result(
            session, status, body, "upsert cons data"
        )

    async def doRequest(
        self,
        session,
        api_call,
        api_params=None,
        request_type="GET",
        body=None,
        headers=None,
    ):
        url_secure = self._generateRequest(api_call, api_params)

        if request_type == "GET":
            return await self._makeGETRequest(session, url_secure)
        else:
            return await self._makePOSTRequest(session, url_secure, body)

    def _generateRequest(self, api_call, api_params=None):
        if api_params is None:
            api_params = {}

        apiHost = self.apiHost
        if self.apiSecurePort != 443:
            apiHost = apiHost + ":" + str(self.apiSecurePort)

        request = RequestGenerator(self.apiId, self.apiSecret, apiHost, True)
        url_secure = request.getUrl(api_call, api_params)
        return url_secure

    async def _makeGETRequest(self, session, url_secure):
        return await self._makeRequest(session, url_secure, "GET")

    async def _makePOSTRequest(self, session, url_secure, body=None):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/xml",
        }
        return await self._makeRequest(session, url_secure, "POST", body, headers)

    async def _makeRequest(
        self, session, url_secure, request_type, http_body=None, headers=None
    ):
        composite_url = "https://" + self.apiHost + url_secure.getPathAndQuery()

        if headers is None:
            headers = {}

        headers["User-Agent"] = "Python API"

        async with session.request(
            request_type, composite_url, data=http_body, headers=headers
        ) as response:
            return response.status, await response.text()

    def check_for_error(self, status, response_body):
        if status != 200:
            raise BsdAPIException(
                "Status code: {}. Message: {}".format(status, response_body)
            )
        d = xmltodict.parse(response_body, attr_prefix="", cdata_key="value")
        api_result = d["api"]
        if api_result.get("errors"):
            message = api_result.get("errors").get("error").get("message")
            raise BsdAPIException("BSD API Error: {}".format(message))
