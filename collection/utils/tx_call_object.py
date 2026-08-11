"""Build call objects from fetched transactions without altering their fields."""

def build_call_object(tx):
    """Return a fresh debug_traceCall/eth_call object."""
    call_obj = {
        "from": tx.get("from"),
        "to": tx.get("to"),
        "gas": tx.get("gas"),
        "value": tx.get("value", "0x0"),
        "data": tx.get("input", "0x"),
    }

    if tx.get("maxFeePerGas"):
        call_obj["maxFeePerGas"] = tx["maxFeePerGas"]
        if tx.get("maxPriorityFeePerGas"):
            call_obj["maxPriorityFeePerGas"] = tx["maxPriorityFeePerGas"]
    elif tx.get("gasPrice"):
        call_obj["gasPrice"] = tx["gasPrice"]

    if tx.get("type") is not None:
        call_obj["type"] = tx["type"]
    if tx.get("chainId"):
        call_obj["chainId"] = tx["chainId"]
    if tx.get("nonce") is not None:
        call_obj["nonce"] = tx["nonce"]
    if tx.get("accessList"):
        call_obj["accessList"] = tx["accessList"]
    if tx.get("authorizationList"):
        call_obj["authorizationList"] = tx["authorizationList"]

    return call_obj
