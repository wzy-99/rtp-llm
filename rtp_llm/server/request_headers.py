"""HTTP header helpers for frontend request metadata."""

from typing import Mapping, Optional

_BEARER_PREFIX = "Bearer "


def extract_api_key(headers: Optional[Mapping[str, str]]) -> Optional[str]:
    """Extract the client-supplied API key from request headers.

    Priority mirrors the flexlb server side (``HttpLoadBalanceServer``):
    ``X-Api-Key`` > ``Api-Key`` > ``Authorization: Bearer <token>``.
    """

    if headers is None:
        return None

    for name in ("X-Api-Key", "Api-Key"):
        value = headers.get(name)
        if value and value.strip():
            return value.strip()

    authorization = headers.get("Authorization") or ""
    if authorization.startswith(_BEARER_PREFIX):
        token = authorization[len(_BEARER_PREFIX) :].strip()
        if token:
            return token

    return None
