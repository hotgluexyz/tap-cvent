# tap-cvent

A [Singer](https://www.singer.io/) tap that extracts data from **Cvent**. It is built with [hotglue-singer-sdk](https://github.com/hotgluexyz/HotglueSingerSDK) and speaks the standard Singer message protocol on stdout, so you can pair it with any compatible target.

## Features

- **REST**-style HTTP streams against the Cvent Event API (see `client.py` / `streams.py`).
- **OAuth2 client credentials** with access token support via Hotglue (`access_token_support` on the tap).
- Configurable **`api_url`** and optional **`start_date`** (see [Configuration](#configuration)).
- Token-based pagination and `lastModified` incremental sync where the endpoint supports it.

### Streams

All paths are relative to `api_url` (default `https://api-platform.cvent.com/ea`). Every
stream uses `id` as its primary key.

Account-wide streams:

| Stream | Endpoint | Replication key | Role |
| ------ | -------- | --------------- | ---- |
| `events` | `GET /events` | `lastModified` | Event context; parent of the event-scoped streams |
| `contacts` | `GET /contacts` | `lastModified` | Address book; matched to DonorPerfect constituents |
| `contact_types` | `GET /contact-types` | full table | Lookup |
| `orders` | `GET /orders` | `lastModified` | Purchase intent, **not** money |
| `transactions` | `GET /transactions` | `lastModified` | Charge/refund headers — the gift source |

Event-scoped streams, synced as children of `events` and stamped with an `event_id`
field. Catalogs and attendees use `?eventId=`; line items use the event id in the path
because Cvent has no account-wide `/orders/items` or `/transactions/items`.

| Stream | Endpoint | Replication key | Role |
| ------ | -------- | --------------- | ---- |
| `order_items` | `GET /events/{event_id}/orders/items` | `lastModified` | Line items purchased |
| `transaction_items` | `GET /events/{event_id}/transactions/items` | `lastModified` | Charge lines carrying `product.{id,type}` |
| `attendees` | `GET /attendees` | `lastModified` | Registrations / participation |
| `registration_types` | `GET /registration-types` | full table | Lookup |
| `registration_paths` | `GET /registration-paths` | full table | Lookup |
| `admission_items` | `GET /admission-items` | full table | Tickets |
| `donation_items` | `GET /donation-items` | full table | Registration donations |
| `quantity_items` | `GET /quantity-items` | full table | Add-ons |
| `membership_items` | `GET /membership-items` | full table | Memberships |
| `fee_items` | `GET /fee-items` | full table | Service fees |
| `program_items` | `GET /program-items` | full table | Sessions |

Cvent exposes no single products endpoint. Each product type is its own event-scoped
catalog, and `transaction_items.product.{id,type}` joins into the matching catalog to
resolve a SKU for GL code and campaign mapping.

### Pagination

List responses wrap records in a `data` array alongside a `paging` object:

```json
{ "paging": { "limit": 100, "totalCount": 500, "currentToken": "ce44b066-…" }, "data": [] }
```

The tap requests 100 records per call and passes `paging.currentToken` back as the
`token` query parameter. Cvent returns a `currentToken` even on the final page, so the
tap stops when a page comes back shorter than the requested limit.

### Incremental sync

Streams with a `lastModified` replication key send a `filter=lastModified gt '…'` query
parameter built from the state bookmark, falling back to `start_date` on the first run.
Not every Cvent endpoint supports filters; if one rejects the parameter, drop the
`replication_key` on that stream so it syncs as a full table.

Selecting any event-scoped stream forces `events` to full-table replication, and the SDK
logs a warning saying so. This is intentional: an event whose own `lastModified` has not
changed can still gain new attendees or transactions, so the parent must be listed in
full to avoid missing children.

### Rate limits

The API returns `429` when the rate limit is exceeded. The SDK retries `429` and `5xx`
responses with exponential backoff. A `403` means the OAuth app is missing a scope for
that endpoint rather than a transient failure.

## Requirements

- Python **3.10+** (see `requires-python` in `pyproject.toml`).

## Installation

1. **Clone** this repository and `cd` into the project directory.
2. **Create `config.json`** in the project root with your credentials and settings (see [Configuration](#configuration) for the fields and an example).
3. **Create a virtual environment** and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Windows, use `.venv\Scripts\activate` instead of `source .venv/bin/activate`.

4. **Install the package** in editable mode:

```bash
pip install -e .
```

5. **Run the tap** (with the venv still activated):

```bash
tap-cvent --help
```

## Configuration

| Setting | Type | Required | Default | Description |
| ------- | ---- | -------- | ------- | ----------- |
| `start_date` | string (datetime) | no | `2000-01-01T00:00:00Z` | Earliest record date to sync. |
| `api_url` | string | no | `https://api-platform.cvent.com/ea` | Base URL for the API. |
| `client_id` | string | yes | — | OAuth client ID. **Sensitive.** |
| `client_secret` | string | yes | — | OAuth client secret. **Sensitive.** |

Cvent uses the client-credentials grant, so there is no refresh token. Access tokens
last 60 minutes and the tap re-runs the token exchange when one expires.

Run `tap-cvent --about` (or `tap-cvent --about --format=markdown`) for the authoritative schema for your installed version.

### Example `config.json`

```json
{
  "start_date": "2000-01-01T00:00:00Z",
  "api_url": "https://api-platform.cvent.com/ea",
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET"
}
```

Do not commit real credentials. Prefer environment variables or a secrets manager in production.

### Environment-based config

You can load settings from the process environment using `--config=ENV` (the SDK merges env into config). Env names follow the tap’s setting keys (see `tap-cvent --about`).

## Usage

With your virtual environment **activated** and `config.json` in place:

Discover stream catalog:

```bash
tap-cvent --config config.json --discover > catalog.json
```

Run a sync (with optional state):

```bash
tap-cvent --config config.json --catalog catalog.json --state state.json
```

Pipe to any Singer target:

```bash
tap-cvent --config config.json --catalog catalog.json | target-jsonl
```

Inspect built-in settings and stream metadata:

```bash
tap-cvent --about
```

## API / documentation

### API hosts

| Region | Base URL |
| ------ | -------- |
| US / global | `https://api-platform.cvent.com/ea` |
| EMEA | `https://api-platform-eur.cvent.com/ea` |

The OAuth token endpoint is `POST {api_url}/oauth2/token`. It takes the client id and
secret as an HTTP Basic header with `grant_type=client_credentials` and `client_id` in a
form-encoded body, and returns a Bearer token valid for 60 minutes.

### Required scopes

The OAuth app needs read scopes for every selected stream:
`event/events:read`, `event/contacts:read`, `event/attendees:read`, `event/orders:read`,
`event/transactions:read`, `event/registration-types:read`,
`event/registration-paths:read`, `event/admission-items:read`,
`event/donation-items:read`, `event/quantity-items:read`,
`event/membership-items:read`, `event/fee-items:read`, and `event/program-items:read`.

### Links

- [Developer quickstart](https://developers.cvent.com/docs/rest-api/tutorials/developer-quickstart)
- [API reference: auth, pagination, error codes](https://developers.cvent.com/docs/rest-api/reference/reference)
- [Filter syntax](https://developers.cvent.com/docs/rest-api/reference/filters)


## License
MIT — see `LICENSE` and `pyproject.toml`.
