"""Stream type classes for tap-cvent.

Endpoint layout (all under ``https://api-platform.cvent.com/ea``):

* Account-wide lists — ``/events``, ``/contacts``, ``/contact-types``, ``/orders``,
  ``/transactions``.
* Event-scoped lists — ``?eventId=<id>`` children of ``EventsStream`` (attendees,
  admission items, program items), plus nested paths under
  ``/events/{event_id}/...`` for line items, registration lookups, and the
  remaining product catalogs.

Cvent has no single products endpoint. Each product type is its own event-scoped
list, and ``transaction_items.product.{id,type}`` joins into the matching catalog
stream to resolve a SKU for GL code and campaign mapping.
"""

from __future__ import annotations

from typing import Any, ClassVar, Iterable

import requests
from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_cvent.client import CventStream


def nested() -> th.CustomType:
    """Return a schema for a Cvent reference object, e.g. ``{"id": ..., "name": ...}``.

    These are kept as raw objects rather than enumerated so downstream ETL can read
    any field the API returns without a tap change.
    """
    return th.CustomType({"type": ["object", "null"]})


def nested_list() -> th.CustomType:
    """Return a schema for an array of Cvent objects, kept raw like ``nested()``."""
    return th.CustomType({"type": ["array", "null"], "items": {"type": "object"}})


def catalog_schema(*extra: th.Property) -> dict:
    """Return the shared id/name/code core for product catalogs.

    Cvent catalogs do not share price/status/currency. Each stream adds the fields
    that endpoint actually returns.
    """
    return th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Product id; joins to transaction_items.product.id",
        ),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("name", th.StringType),
        th.Property("code", th.StringType),
        th.Property("description", th.StringType),
        *extra,
    ).to_dict()


class EventsStream(CventStream):
    """Stream for ``events``.

    Parent of every event-scoped stream; each record supplies an ``event_id``.
    """

    name = "events"
    path = "/events"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("title", th.StringType),
        th.Property("code", th.StringType),
        th.Property("type", th.StringType),
        th.Property("category", nested()),
        th.Property("format", th.StringType),
        th.Property("virtual", th.BooleanType),
        th.Property("start", th.DateTimeType),
        th.Property("end", th.DateTimeType),
        th.Property("closeAfter", th.DateTimeType),
        th.Property("archiveAfter", th.DateTimeType),
        th.Property("launchAfter", th.DateTimeType),
        th.Property("timezone", th.StringType),
        th.Property("defaultLocale", th.StringType),
        th.Property("languages", th.ArrayType(th.StringType)),
        th.Property("currency", th.StringType),
        th.Property("registrationSecurityLevel", th.StringType),
        th.Property("status", th.StringType),
        th.Property("eventStatus", th.StringType),
        th.Property("testMode", th.BooleanType),
        # Planner contacts: [{firstName, lastName, email, deleted}]
        th.Property("planners", nested_list()),
        th.Property("customFields", nested_list()),
        # Public-facing URLs: {invitation, agenda, summary, registration}
        th.Property("_links", nested()),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()

    @override
    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass the event id down to the event-scoped streams."""
        return {"event_id": record["id"]}


class EventChildStream(CventStream):
    """Base class for event-scoped endpoints selected with ``?eventId=``."""

    parent_stream_type = EventsStream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Add the parent event id to the standard list params."""
        params = super().get_url_params(context, next_page_token)
        params["eventId"] = context["event_id"]
        return params

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Stamp the parent event id onto the record."""
        row["event_id"] = context["event_id"]
        return row


class EventNestedStream(CventStream):
    """Event-scoped endpoints whose event id is a path segment, not ``?eventId=``."""

    parent_stream_type = EventsStream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Stamp the parent event id onto the record."""
        row["event_id"] = context["event_id"]
        return row


class ContactsStream(CventStream):
    """Stream for ``contacts`` — the account address book, matched to DP donors."""

    name = "contacts"
    path = "/contacts"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("middleName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("prefix", th.StringType),
        th.Property("suffix", th.StringType),
        th.Property("email", th.StringType),
        th.Property("title", th.StringType),
        th.Property("company", th.StringType),
        th.Property("designation", th.StringType),
        th.Property("gender", th.StringType),
        th.Property("pronouns", th.StringType),
        th.Property("mobilePhone", th.StringType),
        th.Property("workPhone", th.StringType),
        th.Property("homePhone", th.StringType),
        th.Property("primaryAddressType", th.StringType),
        # Address blocks: {line1, line2, city, region, postalCode, country}
        th.Property("homeAddress", nested()),
        th.Property("workAddress", nested()),
        th.Property("contactType", nested()),
        th.Property("source", th.StringType),
        th.Property("primaryLanguage", th.StringType),
        # Cvent returns {optedOut, by}, not a boolean.
        th.Property("optOut", nested()),
        th.Property("deleted", th.BooleanType),
        th.Property("purged", th.BooleanType),
        th.Property("customFields", nested_list()),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class ContactTypesStream(CventStream):
    """Stream for ``contact_types`` — lookup used to label contacts."""

    name = "contact_types"
    path = "/contact-types"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
    ).to_dict()

    @override
    def validate_response(self, response: requests.Response) -> None:
        # Missing event/contact-types:read on this OAuth app; skip rather than fail.
        if response.status_code == 403:
            self.logger.warning(
                "Skipping contact_types: OAuth app lacks event/contact-types:read"
            )
            return
        super().validate_response(response)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code == 403:
            return
        yield from super().parse_response(response)

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        if response.status_code == 403:
            return None
        return super().get_next_page_token(response, previous_token)


class OrdersStream(CventStream):
    """Stream for ``orders`` — purchase intent, not money.

    Gifts are derived from ``transactions``; orders only provide ETL context.
    """

    name = "orders"
    path = "/orders"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("number", th.StringType),
        th.Property("invoiceNumber", th.StringType),
        th.Property("event", nested()),
        th.Property("attendee", nested()),
        th.Property("contact", nested()),
        th.Property("type", th.StringType),
        th.Property(
            "cancelled",
            th.BooleanType,
            description="True after the registration is cancelled; this is not a refund",
        ),
        th.Property("amountOrdered", th.NumberType),
        th.Property("amountPaid", th.NumberType),
        th.Property("amountDue", th.NumberType),
        th.Property("paymentMethod", th.StringType),
        th.Property("discounts", nested_list()),
        th.Property("deleted", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class OrderItemsStream(EventNestedStream):
    """Stream for ``order_items`` — line items on an order."""

    name = "order_items"
    path = "/events/{event_id}/orders/items"
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("order", nested()),
        th.Property("event", nested()),
        th.Property("attendee", nested()),
        # {id, type} where type is AdmissionItem, DonationItem, QuantityItem, etc.
        th.Property("product", nested()),
        th.Property("name", th.StringType),
        th.Property("fee", nested()),
        th.Property(
            "active",
            th.BooleanType,
            description="False after the line is cancelled; paid amount can still remain",
        ),
        th.Property("guest", th.BooleanType),
        th.Property("quantity", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("amountOrdered", th.NumberType),
        th.Property("amountPaid", th.NumberType),
        th.Property("amountDue", th.NumberType),
        th.Property("productPriceTierAmount", th.NumberType),
        th.Property("tiered", th.BooleanType),
        th.Property("generalLedgerItems", nested_list()),
        th.Property("discounts", nested_list()),
        th.Property("deleted", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class TransactionsStream(CventStream):
    """Stream for ``transactions`` — charge and refund headers.

    This is the gift source for DonorPerfect. The tap emits every transaction; the
    ETL decides which ones become gifts.
    """

    name = "transactions"
    path = "/transactions"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("event", nested()),
        th.Property("attendee", nested()),
        th.Property("contact", nested()),
        # Cvent returns orders[], not a single order object.
        th.Property("orders", nested_list()),
        th.Property(
            "paymentType",
            th.StringType,
            description="Offline Charge, Offline Refund, and other Cvent payment types",
        ),
        th.Property("paymentMethod", th.StringType),
        th.Property(
            "success",
            th.BooleanType,
            description="False means the charge did not clear; ETL should not emit a gift",
        ),
        th.Property("amount", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("journalNumber", th.StringType),
        th.Property("batchNumber", th.StringType),
        th.Property("referenceNumber", th.StringType),
        th.Property("paymentNote", th.StringType),
        th.Property("billingAddress", nested()),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class TransactionItemsStream(EventNestedStream):
    """Stream for ``transaction_items`` — charge lines carrying the product SKU."""

    name = "transaction_items"
    path = "/events/{event_id}/transactions/items"
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("uniqueId", th.StringType),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("transaction", nested()),
        th.Property("orderItem", nested()),
        th.Property("event", nested()),
        th.Property("attendee", nested()),
        # {id, type}; join to the catalog stream matching type to resolve the SKU.
        th.Property("product", nested()),
        th.Property("name", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class AttendeesStream(EventChildStream):
    """Stream for ``attendees`` — event participation, including free events."""

    name = "attendees"
    path = "/attendees"
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("event", nested()),
        th.Property("contact", nested()),
        th.Property("admissionItem", nested()),
        th.Property("registrationType", nested()),
        th.Property("registrationPath", nested()),
        th.Property("invitationList", nested()),
        th.Property("status", th.StringType, description="Accepted, Cancelled, ..."),
        th.Property("confirmationNumber", th.StringType),
        th.Property("registeredAt", th.DateTimeType),
        th.Property("registrationCancelledAt", th.DateTimeType),
        th.Property("registrationLastModified", th.DateTimeType),
        th.Property("attendeeLastModified", th.DateTimeType),
        th.Property(
            "credit",
            th.NumberType,
            description="Account credit after cancel; 0 until a refund is recorded",
        ),
        th.Property("guest", th.BooleanType),
        th.Property(
            "primaryId",
            th.StringType,
            description="Attendee id of the primary registrant, when guest is true",
        ),
        th.Property("group", nested()),
        th.Property("checkedIn", th.BooleanType),
        th.Property("unsubscribed", th.BooleanType),
        th.Property("testRecord", th.BooleanType),
        th.Property("deletedGuest", th.BooleanType),
        th.Property("invitedBy", th.StringType),
        th.Property("responseMethod", th.StringType),
        th.Property("webLinks", nested()),
        th.Property("answers", nested_list()),
        th.Property("allowAppointmentPushNotifications", th.BooleanType),
        th.Property("customFields", nested_list()),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class RegistrationTypesStream(EventNestedStream):
    """Stream for ``registration_types`` — lookup for registration categories."""

    name = "registration_types"
    path = "/events/{event_id}/registration-types"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("event", nested()),
        th.Property("name", th.StringType),
        th.Property("code", th.StringType),
        th.Property("description", th.StringType),
        th.Property("registrationPath", nested()),
        th.Property("virtual", th.BooleanType),
        th.Property("openForRegistration", th.BooleanType),
        th.Property("capacity", nested()),
    ).to_dict()


class RegistrationPathsStream(EventNestedStream):
    """Stream for ``registration_paths`` — lookup for registration flows."""

    name = "registration_paths"
    path = "/events/{event_id}/registration-paths"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("name", th.StringType),
        th.Property("code", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()


class AdmissionItemsStream(EventChildStream):
    """Stream for ``admission_items`` — event tickets."""

    name = "admission_items"
    path = "/admission-items"

    schema = catalog_schema(
        th.Property("event", nested()),
        th.Property("allowOptionalSessions", th.BooleanType),
        th.Property("includedSessions", nested_list()),
        th.Property("limitedAvailableSessions", nested_list()),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
    )


class DonationItemsStream(EventNestedStream):
    """Stream for ``donation_items`` — donations collected during registration."""

    name = "donation_items"
    path = "/events/{event_id}/donation-items"

    schema = catalog_schema(
        th.Property("minimumAmountAllowedPerInvitee", th.NumberType),
        th.Property("generalLedger", nested()),
        th.Property("openForRegistration", th.BooleanType),
        th.Property("automaticallyClosesOn", th.StringType),
        th.Property("registrantInformation", th.StringType),
    )


class QuantityItemsStream(EventNestedStream):
    """Stream for ``quantity_items`` — add-ons such as raffle tickets or merchandise."""

    name = "quantity_items"
    path = "/events/{event_id}/quantity-items"

    schema = catalog_schema(
        th.Property("maximumItemsPerInvitee", th.IntegerType),
        th.Property("capacity", th.IntegerType),
        th.Property("closeEventOnCapacityFull", th.BooleanType),
        th.Property("openForRegistration", th.BooleanType),
        th.Property("automaticallyClosesOn", th.StringType),
        th.Property("registrantInformation", th.StringType),
    )


class MembershipItemsStream(EventNestedStream):
    """Stream for ``membership_items`` — memberships sold at registration."""

    name = "membership_items"
    path = "/events/{event_id}/membership-items"

    schema = catalog_schema(
        th.Property("duration", th.StringType),
        th.Property("openForRegistration", th.BooleanType),
    )


class FeeItemsStream(EventNestedStream):
    """Stream for ``fee_items`` — service fees applied to an order."""

    name = "fee_items"
    path = "/events/{event_id}/fee-items"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("event_id", th.StringType, description="Parent event id"),
        th.Property("name", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("product", nested()),
        th.Property("active", th.BooleanType),
        th.Property("display", th.BooleanType),
        th.Property("default", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("lastModifiedBy", th.StringType),
    ).to_dict()


class ProgramItemsStream(EventChildStream):
    """Stream for ``program_items`` — sessions an attendee can register for."""

    name = "program_items"
    path = "/program-items"

    schema = catalog_schema(
        th.Property("start", th.DateTimeType),
        th.Property("end", th.DateTimeType),
        th.Property("capacity", th.IntegerType),
        th.Property("created", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
    )
