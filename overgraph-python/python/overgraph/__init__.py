from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .overgraph import *  # noqa: F401,F403
from .async_api import AsyncOverGraph, AsyncWriteTxn, async_scrub_path  # noqa: F401


def _set_if_not_none(target: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        target[key] = value


def _optional_list(value: Sequence[Any] | None, field: str) -> list[Any] | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{field} must be a list or tuple, not str/bytes")
    if isinstance(value, MappingABC):
        raise TypeError(f"{field} must be a list or tuple, not mapping")
    return list(value)


def _optional_int_list(value: Sequence[int] | None, field: str) -> list[int] | None:
    items = _optional_list(value, field)
    if items is None:
        return None
    for index, item in enumerate(items):
        if isinstance(item, bool) or not isinstance(item, int):
            raise TypeError(f"{field}[{index}] must be int, not {type(item).__name__}")
    return items


def _optional_str_list(value: Sequence[str] | None, field: str) -> list[str] | None:
    items = _optional_list(value, field)
    if items is None:
        return None
    for index, item in enumerate(items):
        if not isinstance(item, str):
            raise TypeError(f"{field}[{index}] must be str, not {type(item).__name__}")
    return items


def _optional_mapping_list(
    value: Sequence[Mapping[str, Any]] | None,
    field: str,
) -> list[dict[str, Any]] | None:
    items = _optional_list(value, field)
    if items is None:
        return None
    parsed = []
    for index, item in enumerate(items):
        if not isinstance(item, MappingABC):
            raise TypeError(f"{field}[{index}] must be mapping, not {type(item).__name__}")
        parsed.append(dict(item))
    return parsed


def _optional_dict(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, MappingABC):
        raise TypeError(f"mapping field must be mapping, not {type(value).__name__}")
    return dict(value)


def _request_to_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, MappingABC):
        return dict(value)
    raise TypeError(f"pattern entries must be request helpers or mappings, not {type(value).__name__}")


def _request_list(value: Sequence[Any], field: str) -> list[dict[str, Any]]:
    items = _optional_list(value, field)
    if items is None:
        return []
    return [_request_to_dict(item) for item in items]


@dataclass(frozen=True)
class NodeQueryRequest:
    label_filter: Mapping[str, Any] | None = None
    ids: Sequence[int] | None = None
    keys: Sequence[str] | None = None
    filter: Mapping[str, Any] | None = None
    order_by: str | None = None
    limit: int | None = None
    after: int | None = None
    allow_full_scan: bool = False

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        _set_if_not_none(data, "label_filter", _optional_dict(self.label_filter))
        _set_if_not_none(data, "ids", _optional_int_list(self.ids, "ids"))
        _set_if_not_none(data, "keys", _optional_str_list(self.keys, "keys"))
        _set_if_not_none(data, "filter", _optional_dict(self.filter))
        _set_if_not_none(data, "order_by", self.order_by)
        _set_if_not_none(data, "limit", self.limit)
        _set_if_not_none(data, "after", self.after)
        if self.allow_full_scan:
            data["allow_full_scan"] = True
        return data


QueryNodeRequest = NodeQueryRequest


@dataclass(frozen=True)
class EdgeQueryRequest:
    label: str | None = None
    ids: Sequence[int] | None = None
    from_ids: Sequence[int] | None = None
    to_ids: Sequence[int] | None = None
    endpoint_ids: Sequence[int] | None = None
    filter: Mapping[str, Any] | None = None
    limit: int | None = None
    after: int | None = None
    allow_full_scan: bool = False

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        _set_if_not_none(data, "label", self.label)
        _set_if_not_none(data, "ids", _optional_int_list(self.ids, "ids"))
        _set_if_not_none(data, "from_ids", _optional_int_list(self.from_ids, "from_ids"))
        _set_if_not_none(data, "to_ids", _optional_int_list(self.to_ids, "to_ids"))
        _set_if_not_none(data, "endpoint_ids", _optional_int_list(self.endpoint_ids, "endpoint_ids"))
        _set_if_not_none(data, "filter", _optional_dict(self.filter))
        _set_if_not_none(data, "limit", self.limit)
        _set_if_not_none(data, "after", self.after)
        if self.allow_full_scan:
            data["allow_full_scan"] = True
        return data


QueryEdgeRequest = EdgeQueryRequest
