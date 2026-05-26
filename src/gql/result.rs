use crate::error::EngineError;
#[cfg(test)]
use crate::row_projection::{ProjectedEdge, ProjectedNode, ProjectedValue};
use crate::types::{
    GqlEdge, GqlNode, GqlPath, GqlValue, GraphEdgeValue, GraphNodeValue, GraphPathValue, GraphValue,
};
use std::collections::BTreeMap;

#[cfg(test)]
pub(crate) fn projected_value_to_gql_value(value: ProjectedValue) -> Result<GqlValue, EngineError> {
    match value {
        ProjectedValue::Null => Ok(GqlValue::Null),
        ProjectedValue::Bool(value) => Ok(GqlValue::Bool(value)),
        ProjectedValue::Int(value) => Ok(GqlValue::Int(value)),
        ProjectedValue::UInt(value) => Ok(GqlValue::UInt(value)),
        ProjectedValue::Float(value) => Ok(GqlValue::Float(value)),
        ProjectedValue::String(value) => Ok(GqlValue::String(value)),
        ProjectedValue::Bytes(value) => Ok(GqlValue::Bytes(value)),
        ProjectedValue::List(values) => Ok(GqlValue::List(
            values
                .into_iter()
                .map(projected_value_to_gql_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ProjectedValue::Map(values) => Ok(GqlValue::Map(projected_map_to_gql_map(values)?)),
        ProjectedValue::Node(node) => Ok(GqlValue::Node(projected_node_to_gql_node(node)?)),
        ProjectedValue::Edge(edge) => Ok(GqlValue::Edge(projected_edge_to_gql_edge(edge)?)),
        ProjectedValue::Path(path) => Ok(GqlValue::Path(GqlPath {
            node_ids: path.nodes.iter().filter_map(|node| node.id).collect(),
            edge_ids: path.edges.iter().filter_map(|edge| edge.id).collect(),
            nodes: Some(
                path.nodes
                    .into_iter()
                    .map(projected_node_to_gql_node)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            edges: Some(
                path.edges
                    .into_iter()
                    .map(projected_edge_to_gql_edge)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        })),
    }
}

pub(crate) fn graph_value_to_gql_value(value: GraphValue) -> Result<GqlValue, EngineError> {
    match value {
        GraphValue::Null => Ok(GqlValue::Null),
        GraphValue::Bool(value) => Ok(GqlValue::Bool(value)),
        GraphValue::Int(value) => Ok(GqlValue::Int(value)),
        GraphValue::UInt(value) | GraphValue::NodeId(value) | GraphValue::EdgeId(value) => {
            Ok(GqlValue::UInt(value))
        }
        GraphValue::Float(value) => Ok(GqlValue::Float(value)),
        GraphValue::String(value) => Ok(GqlValue::String(value)),
        GraphValue::Bytes(value) => Ok(GqlValue::Bytes(value)),
        GraphValue::List(values) => Ok(GqlValue::List(
            values
                .into_iter()
                .map(graph_value_to_gql_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        GraphValue::Map(values) => Ok(GqlValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, graph_value_to_gql_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        )),
        GraphValue::Node(node) => Ok(GqlValue::Node(graph_node_to_gql_node(node)?)),
        GraphValue::Edge(edge) => Ok(GqlValue::Edge(graph_edge_to_gql_edge(edge)?)),
        GraphValue::Path(path) => Ok(GqlValue::Path(graph_path_to_gql_path(path)?)),
    }
}

#[cfg(test)]
fn projected_map_to_gql_map(
    values: BTreeMap<String, ProjectedValue>,
) -> Result<BTreeMap<String, GqlValue>, EngineError> {
    values
        .into_iter()
        .map(|(key, value)| Ok((key, projected_value_to_gql_value(value)?)))
        .collect()
}

#[cfg(test)]
fn projected_node_to_gql_node(node: ProjectedNode) -> Result<GqlNode, EngineError> {
    Ok(GqlNode {
        id: node.id,
        labels: node.labels,
        key: node.key,
        props: node.props.map(projected_map_to_gql_map).transpose()?,
        weight: node.weight,
        created_at: node.created_at,
        updated_at: node.updated_at,
        dense_vector: node.dense_vector,
        sparse_vector: node.sparse_vector,
    })
}

#[cfg(test)]
fn projected_edge_to_gql_edge(edge: ProjectedEdge) -> Result<GqlEdge, EngineError> {
    Ok(GqlEdge {
        id: edge.id,
        from: edge.from,
        to: edge.to,
        label: edge.label,
        props: edge.props.map(projected_map_to_gql_map).transpose()?,
        weight: edge.weight,
        created_at: edge.created_at,
        updated_at: edge.updated_at,
        valid_from: edge.valid_from,
        valid_to: edge.valid_to,
    })
}

fn graph_node_to_gql_node(node: GraphNodeValue) -> Result<GqlNode, EngineError> {
    Ok(GqlNode {
        id: node.id,
        labels: node.labels,
        key: node.key,
        props: node
            .props
            .map(|props| {
                props
                    .into_iter()
                    .map(|(key, value)| Ok((key, graph_value_to_gql_value(value)?)))
                    .collect::<Result<BTreeMap<_, _>, EngineError>>()
            })
            .transpose()?,
        weight: node.weight,
        created_at: node.created_at,
        updated_at: node.updated_at,
        dense_vector: node.dense_vector,
        sparse_vector: node.sparse_vector,
    })
}

fn graph_edge_to_gql_edge(edge: GraphEdgeValue) -> Result<GqlEdge, EngineError> {
    Ok(GqlEdge {
        id: edge.id,
        from: edge.from,
        to: edge.to,
        label: edge.label,
        props: edge
            .props
            .map(|props| {
                props
                    .into_iter()
                    .map(|(key, value)| Ok((key, graph_value_to_gql_value(value)?)))
                    .collect::<Result<BTreeMap<_, _>, EngineError>>()
            })
            .transpose()?,
        weight: edge.weight,
        created_at: edge.created_at,
        updated_at: edge.updated_at,
        valid_from: edge.valid_from,
        valid_to: edge.valid_to,
    })
}

fn graph_path_to_gql_path(path: GraphPathValue) -> Result<GqlPath, EngineError> {
    Ok(GqlPath {
        node_ids: path.node_ids,
        edge_ids: path.edge_ids,
        nodes: path
            .nodes
            .map(|nodes| {
                nodes
                    .into_iter()
                    .map(graph_node_to_gql_node)
                    .collect::<Result<Vec<_>, EngineError>>()
            })
            .transpose()?,
        edges: path
            .edges
            .map(|edges| {
                edges
                    .into_iter()
                    .map(graph_edge_to_gql_edge)
                    .collect::<Result<Vec<_>, EngineError>>()
            })
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_projection::ProjectedPath;

    #[test]
    fn projected_path_converts_to_public_gql_value() {
        let value = projected_value_to_gql_value(ProjectedValue::Path(ProjectedPath {
            nodes: Vec::new(),
            edges: Vec::new(),
        }))
        .unwrap();
        assert!(matches!(value, GqlValue::Path(_)));
    }

    #[test]
    fn graph_paths_convert_recursively_inside_lists_and_maps() {
        let path = GraphValue::Path(GraphPathValue {
            node_ids: vec![1, 2],
            edge_ids: vec![9],
            nodes: None,
            edges: None,
        });
        let value = graph_value_to_gql_value(GraphValue::Map(BTreeMap::from([(
            "paths".to_string(),
            GraphValue::List(vec![path]),
        )])))
        .unwrap();
        let GqlValue::Map(map) = value else {
            panic!("expected map");
        };
        let Some(GqlValue::List(paths)) = map.get("paths") else {
            panic!("expected path list");
        };
        let GqlValue::Path(path) = &paths[0] else {
            panic!("expected path value");
        };
        assert_eq!(path.node_ids, vec![1, 2]);
        assert_eq!(path.edge_ids, vec![9]);
    }
}
