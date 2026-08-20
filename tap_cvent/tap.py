"""Cvent tap class."""

from __future__ import annotations

from typing import Any

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from typing_extensions import override

from tap_cvent.auth import CventAuthenticator
from tap_cvent.client import DEFAULT_API_URL
from tap_cvent.streams import (
    AdmissionItemsStream,
    AttendeesStream,
    ContactsStream,
    ContactTypesStream,
    DonationItemsStream,
    EventsStream,
    FeeItemsStream,
    MembershipItemsStream,
    OrderItemsStream,
    OrdersStream,
    ProgramItemsStream,
    QuantityItemsStream,
    RegistrationPathsStream,
    RegistrationTypesStream,
    TransactionItemsStream,
    TransactionsStream,
)

STREAM_TYPES = [
    # Account-wide
    EventsStream,
    ContactsStream,
    ContactTypesStream,
    OrdersStream,
    TransactionsStream,
    # Event-scoped children of EventsStream
    OrderItemsStream,
    TransactionItemsStream,
    AttendeesStream,
    RegistrationTypesStream,
    RegistrationPathsStream,
    AdmissionItemsStream,
    DonationItemsStream,
    QuantityItemsStream,
    MembershipItemsStream,
    FeeItemsStream,
    ProgramItemsStream,
]


class TapCvent(Tap):
    """Singer tap for Cvent."""

    name = "tap-cvent"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
            default="2000-01-01T00:00:00Z",
        ),
        th.Property(
            "api_url",
            th.StringType,
            description="Base URL for the Cvent API. EMEA accounts use "
            "https://api-platform-eur.cvent.com/ea",
            default=DEFAULT_API_URL,
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="OAuth client ID for the Cvent OAuth app",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="OAuth client secret for the Cvent OAuth app",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @classmethod
    def access_token_support(
        cls,
        connector: Any = None,
    ) -> tuple[type[OAuthAuthenticator], str]:
        """Return the authenticator class and OAuth token endpoint.

        The SDK also calls this without a connector to probe for token support, so the
        endpoint falls back to the default host.

        Returns:
            A tuple with the authenticator class and the OAuth token endpoint URL.
        """
        # The token endpoint sits under the same host as the data API (US vs EMEA).
        config = connector.config if connector else {}
        api_url = config.get("api_url", DEFAULT_API_URL)
        return CventAuthenticator, f"{api_url}/oauth2/token"


if __name__ == "__main__":
    TapCvent.cli()
