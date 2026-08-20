"""Stream type classes for tap-cvent."""

from __future__ import annotations

from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers

from tap_cvent.client import CventStream


class ContactsStream(CventStream):
    """Stream for ``contacts``."""

    name = "contacts"
    path = "/contacts"
    # TODO: Replace with your actual primary key column name(s).
    primary_keys = ["id"]
    # TODO: Replace with your actual replication key, or set to None if not incremental.
    replication_key = "modified_at"
    # TODO: Replace with your actual schema.
    schema = th.PropertiesList(
        # TODO: Add the rest of the properties / fields from the API response (types, nested objects, etc.).
        th.Property(
            "id",
            th.StringType,
            description="TODO: Replace with your actual primary key field and type.",
        ),
        th.Property(
            "modified_at",
            th.DateTimeType,
            description="TODO: Replace with your actual replication key field and type (or remove if full-table).",
        ),
    ).to_dict()


class EventsStream(CventStream):
    """Stream for ``events``."""

    name = "events"
    path = "/events"
    # TODO: Replace with your actual primary key column name(s).
    primary_keys = ["id"]
    # TODO: Replace with your actual replication key, or set to None if not incremental.
    replication_key = "modified_at"
    # TODO: Replace with your actual schema.
    schema = th.PropertiesList(
        # TODO: Add the rest of the properties / fields from the API response (types, nested objects, etc.).
        th.Property(
            "id",
            th.StringType,
            description="TODO: Replace with your actual primary key field and type.",
        ),
        th.Property(
            "modified_at",
            th.DateTimeType,
            description="TODO: Replace with your actual replication key field and type (or remove if full-table).",
        ),
    ).to_dict()


class TransactionsStream(CventStream):
    """Stream for ``transactions``."""

    name = "transactions"
    path = "/transactions"
    # TODO: Replace with your actual primary key column name(s).
    primary_keys = ["id"]
    # TODO: Replace with your actual replication key, or set to None if not incremental.
    replication_key = "modified_at"
    # TODO: Replace with your actual schema.
    schema = th.PropertiesList(
        # TODO: Add the rest of the properties / fields from the API response (types, nested objects, etc.).
        th.Property(
            "id",
            th.StringType,
            description="TODO: Replace with your actual primary key field and type.",
        ),
        th.Property(
            "modified_at",
            th.DateTimeType,
            description="TODO: Replace with your actual replication key field and type (or remove if full-table).",
        ),
    ).to_dict()
