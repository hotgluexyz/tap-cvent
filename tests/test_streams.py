"""Tests for Cvent pagination, query params, and event-scoped child streams."""

import json

import pytest
import requests

from tap_cvent.auth import CventAuthenticator
from tap_cvent.streams import (
    AttendeesStream,
    ContactTypesStream,
    DonationItemsStream,
    EventsStream,
    FeeItemsStream,
    MembershipItemsStream,
    OrderItemsStream,
    QuantityItemsStream,
    RegistrationPathsStream,
    RegistrationTypesStream,
    TransactionItemsStream,
)
from tap_cvent.tap import TapCvent

SAMPLE_CONFIG = {
    "client_id": "placeholder",
    "client_secret": "placeholder",
    "start_date": "2024-01-01T00:00:00Z",
}


@pytest.fixture
def tap():
    return TapCvent(config=SAMPLE_CONFIG, parse_env_config=False)


def make_response(record_count, current_token="tok-2"):
    """Build a Cvent list response carrying ``record_count`` records."""
    response = requests.Response()
    body = {
        "paging": {"limit": 100, "totalCount": record_count, "currentToken": current_token},
        "data": [{"id": str(i)} for i in range(record_count)],
    }
    response._content = json.dumps(body).encode()
    return response


def test_full_page_returns_next_token(tap):
    stream = EventsStream(tap=tap)
    response = make_response(stream.page_size)
    assert stream.get_next_page_token(response, None) == "tok-2"


def test_short_page_ends_pagination(tap):
    """Cvent returns currentToken on the last page, so a short page must stop paging."""
    stream = EventsStream(tap=tap)
    response = make_response(1)
    assert stream.get_next_page_token(response, None) is None


def test_repeated_token_ends_pagination(tap):
    stream = EventsStream(tap=tap)
    response = make_response(stream.page_size, current_token="tok-2")
    assert stream.get_next_page_token(response, "tok-2") is None


def test_url_params_carry_limit_token_and_filter(tap):
    """Without a bookmark the filter must still fall back to the configured start_date."""
    stream = EventsStream(tap=tap)
    params = stream.get_url_params(None, "tok-2")
    assert params["limit"] == stream.page_size
    assert params["token"] == "tok-2"
    assert params["filter"] == "lastModified gt '2024-01-01T00:00:00Z'"


def test_full_table_stream_sends_no_filter(tap):
    stream = ContactTypesStream(tap=tap)
    assert "filter" not in stream.get_url_params(None, None)


def test_contact_types_skips_403(tap):
    """Missing event/contact-types:read must not fail the rest of the tap."""
    stream = ContactTypesStream(tap=tap)
    response = requests.Response()
    response.status_code = 403
    response._content = b'{"message": "Forbidden"}'
    stream.validate_response(response)
    assert list(stream.parse_response(response)) == []
    assert stream.get_next_page_token(response, None) is None


def test_child_context_passes_event_id(tap):
    stream = EventsStream(tap=tap)
    assert stream.get_child_context({"id": "evt-1"}, None) == {"event_id": "evt-1"}


def test_child_stream_filters_by_event_id(tap):
    stream = AttendeesStream(tap=tap)
    params = stream.get_url_params({"event_id": "evt-1"}, None)
    assert params["eventId"] == "evt-1"
    assert stream.parent_stream_type is EventsStream


def test_child_stream_stamps_event_id(tap):
    stream = AttendeesStream(tap=tap)
    row = stream.post_process({"id": "att-1"}, {"event_id": "evt-1"})
    assert row["event_id"] == "evt-1"


def test_nested_item_streams_use_event_path(tap):
    """Line items live under /events/{id}/..., not the 404 account-wide item paths."""
    context = {"event_id": "evt-1"}
    order_items = OrderItemsStream(tap=tap)
    txn_items = TransactionItemsStream(tap=tap)

    assert order_items.parent_stream_type is EventsStream
    assert txn_items.parent_stream_type is EventsStream
    assert order_items.get_url(context).endswith("/events/evt-1/orders/items")
    assert txn_items.get_url(context).endswith("/events/evt-1/transactions/items")
    assert "eventId" not in order_items.get_url_params(context, None)
    assert "eventId" not in txn_items.get_url_params(context, None)
    assert order_items.post_process({"id": "oi-1"}, context)["event_id"] == "evt-1"
    assert txn_items.post_process({"id": "ti-1"}, context)["event_id"] == "evt-1"


def test_nested_lookup_streams_use_event_path(tap):
    """Registration types and paths 404 at /registration-types and /registration-paths."""
    context = {"event_id": "evt-1"}
    types_stream = RegistrationTypesStream(tap=tap)
    paths_stream = RegistrationPathsStream(tap=tap)

    assert types_stream.get_url(context).endswith("/events/evt-1/registration-types")
    assert paths_stream.get_url(context).endswith("/events/evt-1/registration-paths")
    assert "eventId" not in types_stream.get_url_params(context, None)
    assert "eventId" not in paths_stream.get_url_params(context, None)


def test_nested_catalog_streams_use_event_path(tap):
    """Donation, quantity, membership, and fee catalogs 404 at the account-wide paths."""
    context = {"event_id": "evt-1"}
    streams = [
        (DonationItemsStream(tap=tap), "/events/evt-1/donation-items"),
        (QuantityItemsStream(tap=tap), "/events/evt-1/quantity-items"),
        (MembershipItemsStream(tap=tap), "/events/evt-1/membership-items"),
        (FeeItemsStream(tap=tap), "/events/evt-1/fee-items"),
    ]
    for stream, suffix in streams:
        assert stream.get_url(context).endswith(suffix)
        assert "eventId" not in stream.get_url_params(context, None)


def test_token_request_uses_basic_auth_and_client_credentials(tap):
    authenticator = CventAuthenticator(
        EventsStream(tap=tap),
        auth_endpoint="https://api-platform.cvent.com/ea/oauth2/token",
    )
    assert authenticator.oauth_request_body == {
        "grant_type": "client_credentials",
        "client_id": "placeholder",
    }
    assert authenticator.request_auth() == ("placeholder", "placeholder")
