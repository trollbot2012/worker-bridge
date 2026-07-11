"""Secret-safe serialization for worker events and reports."""

from __future__ import annotations

import re
from typing import Any


# (compiled pattern, replacement) pairs. Replacements may reference capture
# groups so structural prefixes (``Authorization:``, the ``user:`` of a
# connection string) survive while the secret itself is masked.
_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # Authorization headers, optionally bearer-prefixed.
    (re.compile(r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?)[^\s,;]+"), r"\1[REDACTED]"),
    # key/token/password/secret assignments.
    (re.compile(r"(?i)((?:api[_-]?key|token|password|passwd|pwd|secret)\s*[:=]\s*)[^\s,;]+"), r"\1[REDACTED]"),
    # Credentials embedded in a URL: scheme://user:PASSWORD@host
    (re.compile(r"(?i)([a-z][a-z0-9+.\-]*://[^:@/\s]+:)[^@/\s]+(@)"), r"\1[REDACTED]\2"),
    # PEM private-key blocks (multi-line).
    (re.compile(r"-----BEGIN [A-Z0-9 ]*?PRIVATE KEY-----.*?-----END [A-Z0-9 ]*?PRIVATE KEY-----", re.DOTALL), "[REDACTED PRIVATE KEY]"),
    # Provider token prefixes (OpenAI, GitHub, Slack, Stripe, ...).
    (re.compile(r"\b(?:sk|rk|pk_live|ghp|gho|ghu|ghs|ghr|github_pat|xox[baprs])[-_][A-Za-z0-9_-]{10,}\b"), "[REDACTED]"),
    # AWS access-key identifiers.
    (re.compile(r"\b(?:AKIA|ASIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA)[0-9A-Z]{16}\b"), "[REDACTED]"),
    # Google API keys.
    (re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b"), "[REDACTED]"),
)


def redact_text(value: str) -> str:
    result = str(value)
    for pattern, replacement in _PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def redact(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            sensitive_key = (
                normalized in {"authorization", "password", "secret", "api_key", "access_token", "refresh_token", "client_secret"}
                or normalized.endswith("_password")
                or normalized.endswith("_secret")
                or normalized.endswith("_api_key")
                or normalized.endswith("_access_token")
                or normalized.endswith("_refresh_token")
            )
            result[str(key)] = "[REDACTED]" if sensitive_key else redact(item)
        return result
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return [redact(item) for item in value]
    return value
