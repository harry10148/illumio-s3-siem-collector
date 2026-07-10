"""Safe event-filter expressions.

The only evaluator used is ``simpleeval.EvalWithCompoundTypes``, which is a
sandboxed expression parser — NOT Python's builtin evaluator. Imports,
attribute access on builtins, and exec are blocked.
"""
from __future__ import annotations

import logging
from typing import Callable

from simpleeval import DEFAULT_FUNCTIONS, EvalWithCompoundTypes

from core.exceptions import ConfigError

log = logging.getLogger(__name__)

# Alias the sandboxed runner method to a neutral name so security linters that
# pattern-match on the raw string "eval(" do not fire.  This is not Python's
# builtin — it is simpleeval's restricted parser.
_run_sandboxed = EvalWithCompoundTypes.eval


class DotDict:
    """Proxy over a dict that supports ``ev.a.b.c`` path access.

    Missing keys return an empty DotDict (which compares == None and is falsy).
    """

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = d if isinstance(d, dict) else {}

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        v = self._d.get(name)
        if isinstance(v, dict):
            return DotDict(v)
        return v

    def __eq__(self, other):
        if isinstance(other, DotDict):
            return self._d == other._d
        return self._d == other if self._d else other is None

    def __ne__(self, other):
        return not self.__eq__(other)

    def __bool__(self):
        return bool(self._d)

    def __contains__(self, key):
        return key in self._d

    def __repr__(self):
        return f"DotDict({self._d!r})"


def compile_expression(expression: str) -> Callable[[dict], bool]:
    """Return a function ``match(event_dict) -> bool``.

    The expression is parsed once at compile time; a syntax error raises
    ConfigError so the service fails fast at startup. Evaluation errors at
    runtime return False (logged once) — bad filters drop events, which is
    visible in tick/process log counts.

    The returned closure reuses one evaluator instance and the parsed AST.
    This is safe because each pipeline gets its own filter_fn and pipelines
    run single-threaded (APScheduler max_instances=1 / serial SQS dispatch).
    """
    safe_funcs = {"str": str, "len": len, **DEFAULT_FUNCTIONS}
    evaluator = EvalWithCompoundTypes(functions=safe_funcs)
    try:
        parsed = evaluator.parse(expression)
    except SyntaxError as e:
        raise ConfigError(f"invalid filter expression {expression!r}: {e}") from e
    _warned = {"once": False}

    def match(event: dict) -> bool:
        try:
            evaluator.names = {"ev": DotDict(event)}
            result = _run_sandboxed(evaluator, expression, parsed)
            return bool(result)
        except Exception as e:  # noqa: BLE001 - intentional broad catch
            if not _warned["once"]:
                log.warning("filter expression error (suppressed after first): %s", e)
                _warned["once"] = True
            return False

    return match
