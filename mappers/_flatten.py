"""Flatten nested dict so downstream parsers (SIEM basic JSON parser) can
key-value extract without understanding nested objects."""
from __future__ import annotations

import json
from typing import Any


def flatten(
    obj: dict,
    separator: str = "_",
    max_depth: int = 10,
    array_strategy: str = "stringify",
) -> dict:
    """Return a new dict where nested dict keys are joined with ``separator``.

    Arrays are handled according to ``array_strategy``:
      - ``"stringify"``: the whole array is JSON-dumped into a string value
      - ``"first"``: use the first element (recursively flattened if it's a dict)
      - ``"skip"``: drop the key entirely

    None is preserved as None (not the string "None").
    When ``max_depth`` is exceeded, the remaining subtree is JSON-stringified.
    """
    if not isinstance(obj, dict):
        raise TypeError("flatten() requires a dict at the top level")

    out: dict[str, Any] = {}
    _walk(obj, out, prefix="", separator=separator,
          depth=0, max_depth=max_depth, array_strategy=array_strategy)
    return out


def _walk(obj: Any, out: dict, prefix: str, separator: str,
          depth: int, max_depth: int, array_strategy: str) -> None:
    if isinstance(obj, dict):
        if not obj and not prefix:
            # top-level empty dict — return nothing
            return
        if not obj:
            out[prefix.rstrip(separator)] = "{}"
            return
        if depth >= max_depth:
            # Stringify each value at this level so we get one more key segment
            for k, v in obj.items():
                out[f"{prefix}{k}"] = json.dumps(v, ensure_ascii=False)
            return
        for k, v in obj.items():
            key = f"{prefix}{k}"
            if isinstance(v, dict):
                _walk(v, out, prefix=key + separator, separator=separator,
                      depth=depth + 1, max_depth=max_depth,
                      array_strategy=array_strategy)
            elif isinstance(v, list):
                _handle_array(v, out, key, separator, depth, max_depth, array_strategy)
            else:
                out[key] = v
    elif isinstance(obj, list):
        _handle_array(obj, out, prefix.rstrip(separator), separator,
                      depth, max_depth, array_strategy)
    else:
        out[prefix.rstrip(separator)] = obj


def _handle_array(arr: list, out: dict, key: str, separator: str,
                  depth: int, max_depth: int, array_strategy: str) -> None:
    if array_strategy == "skip":
        return
    if array_strategy == "first":
        if not arr:
            return
        first = arr[0]
        if isinstance(first, dict):
            _walk(first, out, prefix=key + separator, separator=separator,
                  depth=depth + 1, max_depth=max_depth,
                  array_strategy=array_strategy)
        else:
            out[key] = first
        return
    out[key] = json.dumps(arr, ensure_ascii=False)
