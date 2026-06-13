//! Shared metadata-function vocabulary for GQL.
//!
//! A function call is metadata; a dot access or map key is a user property. This module
//! is the single source of truth for the metadata function names, so expression parsing,
//! semantic validation, lowering, eval, and index DDL cannot drift apart.
//!
//! Function names are matched case-insensitively (callers pass the lowercased name);
//! canonical camelCase spellings are used for display, errors, and element-map keys.

/// Scalar metadata accessor functions: `id(x)`, `elementKey(n)`, `weight(x)`,
/// `createdAt(x)`, `updatedAt(x)`, `validFrom(r)`, `validTo(r)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlMetadataFunction {
    Id,
    ElementKey,
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo,
}

impl GqlMetadataFunction {
    pub(crate) fn from_lower(lower: &str) -> Option<Self> {
        Some(match lower {
            "id" => Self::Id,
            "elementkey" => Self::ElementKey,
            "weight" => Self::Weight,
            "createdat" => Self::CreatedAt,
            "updatedat" => Self::UpdatedAt,
            "validfrom" => Self::ValidFrom,
            "validto" => Self::ValidTo,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::ElementKey => "elementKey",
            Self::Weight => "weight",
            Self::CreatedAt => "createdAt",
            Self::UpdatedAt => "updatedAt",
            Self::ValidFrom => "validFrom",
            Self::ValidTo => "validTo",
        }
    }

    pub(crate) fn valid_for_node(self) -> bool {
        matches!(
            self,
            Self::Id | Self::ElementKey | Self::Weight | Self::CreatedAt | Self::UpdatedAt
        )
    }

    pub(crate) fn valid_for_edge(self) -> bool {
        matches!(
            self,
            Self::Id
                | Self::Weight
                | Self::CreatedAt
                | Self::UpdatedAt
                | Self::ValidFrom
                | Self::ValidTo
        )
    }

    /// Metadata writable via `SET <function>(x) = value`.
    pub(crate) fn writable_for_node(self) -> bool {
        matches!(self, Self::Weight)
    }

    pub(crate) fn writable_for_edge(self) -> bool {
        matches!(self, Self::Weight | Self::ValidFrom | Self::ValidTo)
    }
}

/// Edge endpoint accessors, valid only as the direct argument of `id(...)` for edges
/// (`id(startNode(r))`), and as path functions (`startNode(p)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlEndpointFunction {
    StartNode,
    EndNode,
}

impl GqlEndpointFunction {
    pub(crate) fn from_lower(lower: &str) -> Option<Self> {
        Some(match lower {
            "startnode" => Self::StartNode,
            "endnode" => Self::EndNode,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::StartNode => "startNode",
            Self::EndNode => "endNode",
        }
    }
}

/// Metadata entries allowed in CREATE/MERGE element maps, keyed by the EXACT canonical
/// camelCase spelling (map keys are case-sensitive property names, unlike function calls).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlElementMapMetadataKey {
    ElementKey,
    Weight,
    ValidFrom,
    ValidTo,
}

impl GqlElementMapMetadataKey {
    pub(crate) fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "elementKey" => Self::ElementKey,
            "weight" => Self::Weight,
            "validFrom" => Self::ValidFrom,
            "validTo" => Self::ValidTo,
            _ => return None,
        })
    }

    pub(crate) fn valid_for_node(self) -> bool {
        matches!(self, Self::ElementKey | Self::Weight)
    }

    pub(crate) fn valid_for_edge(self) -> bool {
        matches!(self, Self::Weight | Self::ValidFrom | Self::ValidTo)
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::ElementKey => "elementKey",
            Self::Weight => "weight",
            Self::ValidFrom => "validFrom",
            Self::ValidTo => "validTo",
        }
    }
}
