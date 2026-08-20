"""REST client, including the CventStream base class."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import requests
from hotglue_singer_sdk.authenticators import APIAuthenticatorBase
from hotglue_singer_sdk.streams import RESTStream
from typing_extensions import override

DEFAULT_API_URL = "https://api-platform.cvent.com/ea"


class CventStream(RESTStream):
    """Cvent stream class.

    Every list endpoint returns the same envelope::

        {
          "paging": {"limit": 100, "totalCount": 1, "currentToken": "<uuid>"},
          "data": [ ...records... ]
        }
    """

    records_jsonpath = "$.data[*]"
    page_size = 100

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via the ``api_url`` tap setting."""
        return self.config.get("api_url", DEFAULT_API_URL)

    @override
    @cached_property
    def authenticator(self) -> APIAuthenticatorBase:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        authenticator_cls, auth_endpoint = self._tap.access_token_support(self._tap)
        return authenticator_cls(self, auth_endpoint=auth_endpoint)

    @override
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {"Accept": "application/json"}

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return the paging token for the next page, or None when done.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns:
            The ``paging.currentToken`` to send as the next ``token`` param.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        body = response.json()
        token = body.get("paging", {}).get("currentToken")
        # Cvent echoes currentToken on the final page too, so a partial page — not a
        # missing token — is what reliably marks the end of the result set.
        if token == previous_token or len(body.get("data", [])) < self.page_size:
            return None
        return token

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {"limit": self.page_size}
        if next_page_token:
            params["token"] = next_page_token
        # get_starting_time falls back to the configured start_date when no bookmark
        # has been written yet, which is the case on a stream's first request.
        start_date = self.get_starting_time(context) if self.replication_key else None
        if start_date:
            # Cvent filter grammar is "<field> <operator> '<value>'".
            params["filter"] = (
                f"{self.replication_key} gt '{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
            )
        return params
