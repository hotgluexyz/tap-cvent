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


def make_response(record_count, next_token=None, current_token="cur-1"):
    """Build a Cvent list response carrying ``record_count`` records."""
    response = requests.Response()
    paging = {"limit": 100, "totalCount": record_count, "currentToken": current_token}
    if next_token is not None:
        paging["nextToken"] = next_token
    body = {
        "paging": paging,
        "data": [{"id": str(i)} for i in range(record_count)],
    }
    response._content = json.dumps(body).encode()
    return response


def test_next_token_drives_pagination(tap):
    stream = EventsStream(tap=tap)
    response = make_response(stream.page_size, next_token="tok-2")
    assert stream.get_next_page_token(response, None) == "tok-2"


def test_missing_next_token_ends_pagination(tap):
    """Cvent omits nextToken only on the last page, even when that page is full."""
    stream = EventsStream(tap=tap)
    response = make_response(stream.page_size)
    assert stream.get_next_page_token(response, None) is None


def test_current_token_is_not_used_as_next_page(tap):
    """currentToken names the page just received; paging on it refetches that page."""
    stream = EventsStream(tap=tap)
    response = make_response(stream.page_size, next_token="tok-2", current_token="cur-1")
    assert stream.get_next_page_token(response, "cur-1") == "tok-2"


def test_short_page_still_pages_when_next_token_present(tap):
    """Page length must not gate pagination; Cvent may return a short non-final page."""
    stream = EventsStream(tap=tap)
    response = make_response(1, next_token="tok-2")
    assert stream.get_next_page_token(response, None) == "tok-2"


def walk_pages(stream, pages, monkeypatch):
    """Drive the SDK paging loop over ``pages``, keyed by the token that requests each.

    Returns the records yielded and the query params of every request made, so tests
    can assert both the data and the request sequence.
    """
    params_seen = []

    def fake_prepare_request(context, next_page_token=None):
        params = stream.get_url_params(context, next_page_token)
        params_seen.append(params)
        return params

    def fake_request(prepared_request, context):
        record_count, next_token = pages[prepared_request.get("token")]
        return make_response(record_count, next_token=next_token)

    monkeypatch.setattr(stream, "prepare_request", fake_prepare_request)
    monkeypatch.setattr(stream, "_request", fake_request)
    return list(stream.request_records(None)), params_seen


def test_pagination_walks_every_page(tap, monkeypatch):
    """Three pages must yield every record exactly once, following nextToken each time."""
    stream = EventsStream(tap=tap)
    pages = {
        None: (stream.page_size, "tok-2"),
        "tok-2": (stream.page_size, "tok-3"),
        "tok-3": (37, None),
    }
    records, params_seen = walk_pages(stream, pages, monkeypatch)

    assert len(records) == 237
    assert [p.get("token") for p in params_seen] == [None, "tok-2", "tok-3"]


def test_pagination_stops_on_last_full_page(tap, monkeypatch):
    """The old currentToken bug refetched page 1 here, yielding 200 rows and 100 ids."""
    stream = EventsStream(tap=tap)
    records, params_seen = walk_pages(stream, {None: (stream.page_size, None)}, monkeypatch)

    assert len(params_seen) == 1
    assert len(records) == stream.page_size
    assert len({r["id"] for r in records}) == stream.page_size


def test_pagination_handles_empty_trailing_page(tap, monkeypatch):
    """Cvent may hand back a nextToken whose page is empty when totals divide evenly."""
    stream = EventsStream(tap=tap)
    pages = {None: (stream.page_size, "tok-2"), "tok-2": (0, None)}
    records, params_seen = walk_pages(stream, pages, monkeypatch)

    assert len(records) == stream.page_size
    assert [p.get("token") for p in params_seen] == [None, "tok-2"]


def test_pagination_requests_first_page_without_token(tap, monkeypatch):
    stream = EventsStream(tap=tap)
    _, params_seen = walk_pages(stream, {None: (5, None)}, monkeypatch)

    assert "token" not in params_seen[0]
    assert params_seen[0]["limit"] == stream.page_size
    assert stream.page_size <= 200, "Cvent rejects a limit above 200"


def test_repeated_next_token_raises_rather_than_looping(tap, monkeypatch):
    """A server-side token loop must fail loudly instead of spinning or truncating."""
    stream = EventsStream(tap=tap)
    pages = {None: (stream.page_size, "tok-2"), "tok-2": (stream.page_size, "tok-2")}

    with pytest.raises(RuntimeError, match="Loop detected in pagination"):
        walk_pages(stream, pages, monkeypatch)


class FakeCventList:
    """A Cvent list endpoint: UUID page tokens, nextToken present only when more remain.

    ``empty_trailing_page`` reproduces the documented case where an evenly divisible
    result set hands back a nextToken whose page turns out to be empty.
    """

    def __init__(self, total, page_size, empty_trailing_page=False):
        self.page_size = page_size
        self.requests = 0
        self.offsets = {}
        offsets = list(range(0, total, page_size)) or [0]
        if empty_trailing_page and total and total % page_size == 0:
            offsets.append(total)
        for position, offset in enumerate(offsets):
            token = None if position == 0 else f"{offset:08d}-0000-4000-8000-000000000000"
            following = offsets[position + 1] if position + 1 < len(offsets) else None
            self.offsets[token] = (offset, following)
        self.total = total

    def serve(self, prepared_request, _context):
        self.requests += 1
        offset, following = self.offsets[prepared_request.get("token")]
        records = [{"id": str(i)} for i in range(offset, min(offset + self.page_size, self.total))]
        next_token = None if following is None else f"{following:08d}-0000-4000-8000-000000000000"
        paging = {"limit": self.page_size, "totalCount": self.total, "currentToken": "cur"}
        if next_token:
            paging["nextToken"] = next_token
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({"paging": paging, "data": records}).encode()
        return response


@pytest.mark.parametrize("total", [0, 1, 99, 100, 101, 199, 200, 201, 250, 1000, 2305])
@pytest.mark.parametrize("empty_trailing_page", [False, True])
def test_pagination_retrieves_exact_total(tap, monkeypatch, total, empty_trailing_page):
    """Across every page boundary the tap must return each record once and then stop."""
    stream = EventsStream(tap=tap)
    api = FakeCventList(total, stream.page_size, empty_trailing_page)

    monkeypatch.setattr(
        stream,
        "prepare_request",
        lambda context, next_page_token=None: stream.get_url_params(context, next_page_token),
    )
    monkeypatch.setattr(stream, "_request", api.serve)
    records = list(stream.request_records(None))

    assert len(records) == total
    assert len({r["id"] for r in records}) == total
    assert api.requests == len(api.offsets)


def test_child_stream_pagination_keeps_event_scope(tap, monkeypatch):
    """Paging a child stream must not drop the event scoping on later pages."""
    stream = AttendeesStream(tap=tap)
    params_seen = []

    def fake_prepare_request(context, next_page_token=None):
        params = stream.get_url_params(context, next_page_token)
        params_seen.append(params)
        return params

    def fake_request(prepared_request, context):
        record_count, next_token = {None: (100, "tok-2"), "tok-2": (3, None)}[
            prepared_request.get("token")
        ]
        return make_response(record_count, next_token=next_token)

    monkeypatch.setattr(stream, "prepare_request", fake_prepare_request)
    monkeypatch.setattr(stream, "_request", fake_request)
    records = list(stream.request_records({"event_id": "evt-1"}))

    assert len(records) == 103
    assert [p["eventId"] for p in params_seen] == ["evt-1", "evt-1"]


def test_url_params_carry_limit_token_and_filter(tap):
    """Without a bookmark the filter must still fall back to the configured start_date."""
    stream = EventsStream(tap=tap)
    params = stream.get_url_params(None, "tok-2")
    assert params["limit"] == stream.page_size
    assert params["token"] == "tok-2"
    assert params["filter"] == "lastModified gt '2024-01-01T00:00:00Z'"


def test_events_filter_ands_single_event_id():
    """Configured event_ids must AND onto the lastModified filter."""
    tap = TapCvent(
        config={**SAMPLE_CONFIG, "event_ids": ["evt-1"]},
        parse_env_config=False,
    )
    stream = EventsStream(tap=tap)
    params = stream.get_url_params(None, None)
    assert params["filter"] == (
        "lastModified gt '2024-01-01T00:00:00Z' and id eq 'evt-1'"
    )


def test_events_filter_ors_multiple_event_ids():
    tap = TapCvent(
        config={**SAMPLE_CONFIG, "event_ids": ["evt-1", "evt-2"]},
        parse_env_config=False,
    )
    stream = EventsStream(tap=tap)
    params = stream.get_url_params(None, None)
    assert params["filter"] == (
        "lastModified gt '2024-01-01T00:00:00Z' and "
        "(id eq 'evt-1' or id eq 'evt-2')"
    )


def test_events_filter_omitted_when_event_ids_empty():
    tap = TapCvent(
        config={**SAMPLE_CONFIG, "event_ids": []},
        parse_env_config=False,
    )
    stream = EventsStream(tap=tap)
    params = stream.get_url_params(None, None)
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
