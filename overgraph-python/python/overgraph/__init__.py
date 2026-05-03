from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .overgraph import *  # noqa: F401,F403
from .async_api import AsyncOverGraph, AsyncWriteTxn  # noqa: F401


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
    type_id: int | None = None
    ids: Sequence[int] | None = None
    keys: Sequence[str] | None = None
    filter: Mapping[str, Any] | None = None
    order_by: str | None = None
    limit: int | None = None
    after: int | None = None
    allow_full_scan: bool = False

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        _set_if_not_none(data, "type_id", self.type_id)
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
class GraphNodePattern:
    alias: str
    type_id: int | None = None
    ids: Sequence[int] | None = None
    keys: Sequence[str] | None = None
    filter: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"alias": self.alias}
        _set_if_not_none(data, "type_id", self.type_id)
        _set_if_not_none(data, "ids", _optional_int_list(self.ids, "ids"))
        _set_if_not_none(data, "keys", _optional_str_list(self.keys, "keys"))
        _set_if_not_none(data, "filter", _optional_dict(self.filter))
        return data


@dataclass(frozen=True)
class GraphEdgePattern:
    from_alias: str
    to_alias: str
    alias: str | None = None
    direction: str | None = None
    type_filter: Sequence[int] | None = None
    where: Mapping[str, Any] | None = None
    predicates: Sequence[Mapping[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "from_alias": self.from_alias,
            "to_alias": self.to_alias,
        }
        _set_if_not_none(data, "alias", self.alias)
        _set_if_not_none(data, "direction", self.direction)
        _set_if_not_none(data, "type_filter", _optional_int_list(self.type_filter, "type_filter"))
        _set_if_not_none(data, "where", _optional_dict(self.where))
        _set_if_not_none(
            data,
            "predicates",
            _optional_mapping_list(self.predicates, "predicates"),
        )
        return data


@dataclass(frozen=True)
class GraphPatternRequest:
    nodes: Sequence[GraphNodePattern | Mapping[str, Any]]
    edges: Sequence[GraphEdgePattern | Mapping[str, Any]]
    limit: int
    at_epoch: int | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "nodes": _request_list(self.nodes, "nodes"),
            "edges": _request_list(self.edges, "edges"),
            "limit": self.limit,
        }
        _set_if_not_none(data, "at_epoch", self.at_epoch)
        return data
