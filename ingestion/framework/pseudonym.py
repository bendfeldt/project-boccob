"""Keyed pseudonymisation of person identifiers.

``pseudonymise(value, pepper)`` = hex(HMAC-SHA256(pepper, normalised value)).
The pepper is a runtime secret (OpenBao -> env ``PSEUDONYM_PEPPER``); without it the pseudonym
cannot be recomputed or brute-forced from the (small) CPR number space. The same function is used
by the index stage and by the erasure tool to locate a data subject.
"""

from __future__ import annotations

import hashlib
import hmac
import re

_CPR_SHAPE = re.compile(r"^\d{6}-?\d{4}$")


def normalise_identifier(value: str) -> str:
    """Canonical form before hashing: trimmed; CPR numbers without the dash."""
    value = value.strip()
    if _CPR_SHAPE.match(value):
        return value.replace("-", "")
    return value


def pseudonymise(value: str, pepper: str) -> str:
    if not pepper:
        raise ValueError("pseudonymisation pepper is empty")
    digest = hmac.new(pepper.encode("utf-8"), normalise_identifier(value).encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()
