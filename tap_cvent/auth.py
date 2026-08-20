"""Cvent Authentication."""

from __future__ import annotations

from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing_extensions import override


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class CventAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Cvent.

    Cvent uses the OAuth2 client-credentials grant with no refresh token. The
    client secret is sent as an HTTP Basic header on the token request rather
    than in the form body, so the body only carries the grant type and client id.
    Access tokens expire after 60 minutes; the SDK re-runs this exchange on expiry.
    """

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Cvent API.

        Returns:
            A dict with the request body
        """
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
        }

    @override
    def request_auth(self) -> tuple[str, str]:
        """Send the credentials as Basic auth on the token request.

        Returns:
            The (client_id, client_secret) pair used for the Basic header.
        """
        return self.client_id, self.client_secret
