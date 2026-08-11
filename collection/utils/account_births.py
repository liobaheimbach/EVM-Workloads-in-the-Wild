"""Exact account transitions from Reth prestateTracer diff output."""

from typing import Any


def _normalize_address(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError(f"account address is not a string: {value!r}")
    address = value.lower().removeprefix("0x")
    if len(address) != 40 or any(c not in "0123456789abcdef" for c in address):
        raise ValueError(f"invalid account address: {value!r}")
    return address


def _account_map(value: Any, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} account map is not an object")
    normalized = {}
    for raw_address, account in value.items():
        address = _normalize_address(raw_address)
        if address in normalized:
            raise ValueError(f"duplicate normalized address in {label}: {address}")
        if not isinstance(account, dict):
            raise ValueError(f"{label} account entry is not an object: {address}")
        normalized[address] = account
    return normalized


def _quantity(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} is not a quantity: {value!r}")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and value.startswith("0x") and len(value) > 2:
        try:
            return int(value[2:], 16)
        except ValueError:
            pass
    raise ValueError(f"{label} is not a hex quantity: {value!r}")


def _code(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"{label} is not hex bytecode: {value!r}")
    payload = value[2:]
    if len(payload) % 2 or any(c not in "0123456789abcdefABCDEF" for c in payload):
        raise ValueError(f"{label} is not hex bytecode: {value!r}")
    return payload.lower()


def _exists(account: dict[str, Any], label: str) -> bool:
    balance = _quantity(account.get("balance", "0x0"), f"{label}.balance")
    nonce = _quantity(account.get("nonce", "0x0"), f"{label}.nonce")
    code = _code(account.get("code", "0x"), f"{label}.code")
    return balance != 0 or nonce != 0 or bool(code)


def account_transition_counts(pre_value: Any, post_value: Any) -> tuple[int, int]:
    """Return EIP-161 empty-to-live births and live-to-empty deaths."""
    pre = _account_map(pre_value, "pre")
    post = _account_map(post_value, "post")
    births = 0
    deaths = 0
    for address in pre.keys() | post.keys():
        before = _exists(pre[address], f"pre[{address}]") if address in pre else False
        if address in post:
            after_account = dict(pre.get(address, {}))
            after_account.update(post[address])
            after = _exists(after_account, f"post[{address}]")
        else:
            after = False
        births += int(not before and after)
        deaths += int(before and not after)
    return births, deaths


def account_transition_counts_from_trace(result: Any) -> tuple[int, int]:
    if not isinstance(result, dict) or "pre" not in result or "post" not in result:
        raise ValueError("prestate trace lacks pre/post account maps")
    return account_transition_counts(result["pre"], result["post"])
