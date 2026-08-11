"""EIP-7702 authorization accounting derived from protocol inputs.

The authority of an authorization tuple is recovered from the signature; it is
NOT the tuple's `address` field, which is the delegation target. Refund
eligibility additionally depends on the authority's state immediately before the
transaction (existence, nonce, code), so a caller must supply that state.

Charging model (EIP-7702):
  intrinsic: PER_EMPTY_ACCOUNT_COST (25,000) per tuple, always.
  refund:    PER_AUTH_BASE_COST (12,500) per accepted tuple whose authority
             already existed.
The refund joins the global refund counter and is therefore subject to the
single EIP-3529 cap of gross_gas // 5, together with execution/SSTORE refunds.
Authorization processing precedes EVM execution, so the authorization refund
survives a reverted execution while the SSTORE refund does not.
"""
import rlp
from eth_keys import keys
from eth_keys.exceptions import BadSignature
from eth_utils import keccak

MAGIC = b"\x05"
SECP256K1N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
PER_EMPTY_ACCOUNT_COST = 25000
PER_AUTH_BASE_COST = 12500
DELEGATION_PREFIX = bytes.fromhex("ef0100")
DELEGATION_DESIGNATOR_LEN = 23


def to_int(v):
    """Coerce int / hex-str / bytes. web3 returns HexBytes for r and s."""
    if v is None:
        return 0
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, (bytes, bytearray)):
        return int.from_bytes(bytes(v), "big") if v else 0
    s = str(v)
    if s.startswith("0x"):
        return int(s, 16) if len(s) > 2 else 0
    return int(s) if s else 0


def to_bytes(v):
    if v is None:
        return b""
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, int):
        return v.to_bytes(20, "big")
    s = str(v)
    if s.startswith("0x"):
        s = s[2:]
    if len(s) % 2:
        s = "0" + s
    return bytes.fromhex(s) if s else b""


def recover_authority(auth):
    """Recover the authority address from an authorization tuple.

    The signed payload is keccak(MAGIC || rlp([chain_id, address, nonce])).
    Returns a lowercase 0x address, or None when the tuple is malformed or the
    signature violates EIP-2 / EIP-7702 bounds.
    """
    target = to_bytes(auth.get("address"))
    if len(target) != 20:
        return None

    y_parity = to_int(auth.get("yParity") if auth.get("yParity") is not None
                      else auth.get("v"))
    r = to_int(auth.get("r"))
    s = to_int(auth.get("s"))
    if y_parity not in (0, 1):
        return None
    if not (1 <= r < SECP256K1N) or not (1 <= s < SECP256K1N):
        return None
    if s > SECP256K1N // 2:
        return None

    msg = keccak(MAGIC + rlp.encode([to_int(auth.get("chainId")), target,
                                     to_int(auth.get("nonce"))]))
    try:
        sig = keys.Signature(vrs=(y_parity, r, s))
        return sig.recover_public_key_from_msg_hash(msg).to_address().lower()
    except (BadSignature, ValueError):
        return None


def is_delegation_designator(code):
    b = to_bytes(code)
    return len(b) == DELEGATION_DESIGNATOR_LEN and b.startswith(DELEGATION_PREFIX)


def authorization_refund(tx, get_authority_state):
    """Authorization refund for a transaction, from protocol inputs only.

    tx: transaction dict carrying `authorizationList` and `chainId`.
    get_authority_state(address) -> {'exists': bool, 'nonce': int, 'code': bytes}
        describing the authority immediately BEFORE this transaction.

    Tuples are processed in order; an accepted tuple's nonce and code
    transition is applied before the next tuple is evaluated, so repeated
    authorities with sequential nonces behave as the protocol specifies.

    Returns (refund_gas, per_tuple_detail).
    """
    auth_list = tx.get("authorizationList") or []
    tx_chain_id = to_int(tx.get("chainId"))
    overlay = {}

    # The sender's nonce increments before authorization processing, so a
    # self-sponsored tuple carries the already-incremented value.
    sender = tx.get("from")
    if sender:
        sender = str(sender).lower()
        st = get_authority_state(sender)
        if st is not None:
            overlay[sender] = {"exists": st.get("exists"),
                               "nonce": to_int(st.get("nonce")) + 1,
                               "code": to_bytes(st.get("code"))}

    def state_of(addr):
        """Pre-tuple state, or None when unavailable.

        None must not become a zero-nonce account: a rejected tuple would then
        pass the nonce and code checks.
        """
        if addr in overlay:
            return overlay[addr]
        st = get_authority_state(addr)
        if st is None:
            return None
        return {"exists": st.get("exists"),
                "nonce": to_int(st.get("nonce")),
                "code": to_bytes(st.get("code"))}

    refund = 0
    detail = []
    for i, auth in enumerate(auth_list):
        rec = {"index": i, "authority": None, "valid": False,
               "existing": False, "refund": 0, "reason": None}

        auth_chain = to_int(auth.get("chainId"))
        if auth_chain != 0 and auth_chain != tx_chain_id:
            rec["reason"] = "chain_id_mismatch"
            detail.append(rec)
            continue

        authority = recover_authority(auth)
        if authority is None:
            rec["reason"] = "bad_signature"
            detail.append(rec)
            continue
        rec["authority"] = authority

        st = state_of(authority)
        if st is None:
            # An accepted tuple always mutates its authority, so an authority
            # absent from the diff was rejected. Ineligible, not nonexistent.
            rec["reason"] = "authority_absent_from_diff"
            detail.append(rec)
            continue
        if st["code"] and not is_delegation_designator(st["code"]):
            rec["reason"] = "code_not_empty_or_delegated"
            detail.append(rec)
            continue
        if to_int(auth.get("nonce")) != st["nonce"]:
            rec["reason"] = "nonce_mismatch"
            detail.append(rec)
            continue

        rec["valid"] = True
        if st["exists"] is None:
            # No refund credited, but the tuple applied: the overlay below must
            # still run or a later tuple sees a stale nonce.
            rec["reason"] = "existence_undecidable"
        elif st["exists"]:
            rec["existing"] = True
            rec["refund"] = PER_AUTH_BASE_COST
            refund += PER_AUTH_BASE_COST

        target = to_bytes(auth.get("address"))
        overlay[authority] = {
            "exists": True,
            "nonce": st["nonce"] + 1,
            "code": b"" if target == b"\x00" * 20 else DELEGATION_PREFIX + target,
        }
        detail.append(rec)

    return refund, detail


def authority_state_from_parity_statediff(state_diff, address):
    """Pre-transaction authority state from a Parity `trace_replayTransaction`
    stateDiff.

    Field markers are "+" (created), "-" (deleted), "*" ({from, to} modified),
    or "=" (unchanged). Existence is therefore explicit rather than inferred:
    an account whose fields are all "+" did not exist before the transaction.

    Returns None when the address is absent from the diff. An accepted tuple
    always bumps its authority's nonce, so an authority missing from the diff
    had its tuple rejected -- most often because an earlier transaction in the
    same block already consumed that nonce. Callers must treat None as
    "ineligible", not as "does not exist": the account may well exist, and a
    block-minus-one lookup would wrongly find the stale nonce and credit a
    refund the protocol never granted.
    """
    entry = state_diff.get(address) or state_diff.get(address.lower())
    if entry is None:
        return None

    markers = set()
    for field in ("balance", "nonce", "code"):
        v = entry.get(field)
        if isinstance(v, dict):
            markers.update(v.keys())

    UNCHANGED = object()

    def pre_value(field):
        """Pre-transaction value, or UNCHANGED when the diff does not say.

        A field rendered as "=" was not touched, which reveals nothing about its
        value: it is neither a statement that the field is zero nor that it is
        nonzero.
        """
        v = entry.get(field)
        if not isinstance(v, dict):
            return UNCHANGED
        if "+" in v:
            return None
        if "*" in v:
            return v["*"].get("from")
        if "-" in v:
            return v["-"]
        return UNCHANGED

    raw_nonce = pre_value("nonce")
    raw_code = pre_value("code")
    raw_balance = pre_value("balance")

    pre_nonce = 0 if raw_nonce in (None, UNCHANGED) else to_int(raw_nonce or "0x0")
    pre_code = b"" if raw_code in (None, UNCHANGED) else to_bytes(raw_code or "0x")

    # "=" means untouched, which reveals nothing about the value: Parity emits it
    # both for an unchanged nonzero field and for one that was and stays zero.
    # Existence is therefore undecidable unless some field is decisive.
    def decide(raw):
        if raw is None:
            return False
        if raw is UNCHANGED:
            return None
        return to_int(raw or "0x0") != 0

    votes = [decide(raw_nonce), decide(raw_code), decide(raw_balance)]
    if any(v is True for v in votes):
        exists = True
    elif any(v is None for v in votes):
        exists = None
    else:
        exists = False

    return {
        "exists": exists,
        "nonce": pre_nonce,
        "code": pre_code,
    }


def resolve_existence(state, address, get_balance):
    """Settle an undecidable `exists` with a balance read.

    The state diff renders an untouched field as "=", which cannot distinguish a
    nonzero field from one that was and stays zero, so `exists` comes back None.
    get_balance(address) must read the state before the transaction.
    """
    if state is None or state.get("exists") is not None:
        return state
    balance = to_int(get_balance(address))
    return {**state, "exists": bool(balance or state["nonce"] or state["code"])}


def balances_before_each_tx(ordered_state_diffs, parent_balances):
    """Per-transaction authority balances, reconstructed in transaction order.

    A parent-block balance read is only correct for txIndex 0: an earlier
    transaction in the same block may move the balance across zero, which flips
    the authority's existence. Replaying the block's own diffs in order fixes
    this without extra traces.

    ordered_state_diffs: the block's Parity stateDiffs, transaction-index order.
    parent_balances: {address: balance} at the parent block, for the addresses
        of interest.

    Returns a list, one dict per transaction, of the balances in force BEFORE
    that transaction.
    """
    current = {str(a).lower(): to_int(b) for a, b in (parent_balances or {}).items()}
    out = []
    for sd in ordered_state_diffs or []:
        out.append(dict(current))
        for addr, entry in (sd or {}).items():
            v = (entry or {}).get("balance")
            if not isinstance(v, dict):
                continue
            key = str(addr).lower()
            if "+" in v:
                current[key] = to_int(v["+"])
            elif "*" in v:
                current[key] = to_int(v["*"].get("to"))
            elif "-" in v:
                current[key] = 0
    return out


def combine_refunds(authorization_refund_gas, trace_refund_first,
                    trace_refund_final, receipt_failed, gross_gas):
    """Client-independent effective refund.

    Clients disagree on whether the authorization refund appears in the trace's
    refund counter: Erigon credits it before the first opcode, Reth reports zero
    throughout. Subtracting the counter's initial value isolates the execution
    (SSTORE) refund on either client, and the authorization refund is then added
    from the protocol derivation instead of being read out of the trace.

    Authorization processing precedes EVM execution, so the authorization refund
    survives a reverted execution; the SSTORE refund does not. Both share the
    single EIP-3529 cap.

    Returns (uncapped, effective, execution_refund).
    """
    if receipt_failed:
        execution_refund = 0
    else:
        execution_refund = max(0, to_int(trace_refund_final)
                               - to_int(trace_refund_first))
    uncapped = to_int(authorization_refund_gas) + execution_refund
    effective = min(uncapped, to_int(gross_gas) // 5)
    return uncapped, effective, execution_refund
