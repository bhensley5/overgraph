use crate::error::EngineError;
use crate::gql::ast::{
    GqlIndexStatement, GqlPropertyIndexEndpointFunction, GqlPropertyIndexField,
    GqlPropertyIndexKind, GqlPropertyIndexMetadataFunction, GqlPropertyIndexTarget,
    GqlShowPropertyIndexScope,
};
use crate::types::{
    validate_label_token_name, EdgeMetadataIndexField, GqlSemanticErrorCode,
    NodeMetadataIndexField, SecondaryIndexField, SecondaryIndexKind, SecondaryIndexSpec,
    SourceSpan,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlIndexSemanticPlan {
    Create(GqlBoundPropertyIndexStatement),
    Drop(GqlBoundPropertyIndexStatement),
    Show {
        scope: GqlShowPropertyIndexScope,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundPropertyIndexStatement {
    pub(crate) target_kind: GqlPropertyIndexTargetKind,
    pub(crate) label: String,
    pub(crate) fields: Vec<SecondaryIndexField>,
    pub(crate) kind: SecondaryIndexKind,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlPropertyIndexTargetKind {
    Node,
    Edge,
}

pub(crate) fn bind_index_statement(
    statement: GqlIndexStatement,
) -> Result<GqlIndexSemanticPlan, EngineError> {
    Ok(match statement {
        GqlIndexStatement::Create(statement) => GqlIndexSemanticPlan::Create(
            bind_property_index_target(statement.target, statement.kind, statement.span)?,
        ),
        GqlIndexStatement::Drop(statement) => GqlIndexSemanticPlan::Drop(
            bind_property_index_target(statement.target, statement.kind, statement.span)?,
        ),
        GqlIndexStatement::Show(statement) => GqlIndexSemanticPlan::Show {
            scope: statement.scope,
            span: statement.span,
        },
    })
}

pub(crate) fn index_statement_is_mutating(statement: &GqlIndexStatement) -> bool {
    matches!(
        statement,
        GqlIndexStatement::Create(_) | GqlIndexStatement::Drop(_)
    )
}

#[allow(dead_code)]
pub(crate) fn gql_index_target_kind_name(kind: GqlPropertyIndexTargetKind) -> &'static str {
    match kind {
        GqlPropertyIndexTargetKind::Node => "node",
        GqlPropertyIndexTargetKind::Edge => "edge",
    }
}

fn bind_property_index_target(
    target: GqlPropertyIndexTarget,
    kind: GqlPropertyIndexKind,
    span: SourceSpan,
) -> Result<GqlBoundPropertyIndexStatement, EngineError> {
    let kind = match kind {
        GqlPropertyIndexKind::Equality => SecondaryIndexKind::Equality,
        GqlPropertyIndexKind::Range => SecondaryIndexKind::Range,
    };
    match target {
        GqlPropertyIndexTarget::Node {
            variable,
            label,
            fields,
            ..
        } => {
            validate_index_label(&label.name, &label.span)?;
            let fields =
                bind_property_index_fields(GqlPropertyIndexTargetKind::Node, &variable, fields)?;
            validate_bound_index_spec(
                GqlPropertyIndexTargetKind::Node,
                &fields,
                &kind,
                span.clone(),
            )?;
            Ok(GqlBoundPropertyIndexStatement {
                target_kind: GqlPropertyIndexTargetKind::Node,
                label: label.name,
                fields,
                kind,
                span,
            })
        }
        GqlPropertyIndexTarget::Edge {
            variable,
            label,
            fields,
            ..
        } => {
            validate_index_label(&label.name, &label.span)?;
            let fields =
                bind_property_index_fields(GqlPropertyIndexTargetKind::Edge, &variable, fields)?;
            validate_bound_index_spec(
                GqlPropertyIndexTargetKind::Edge,
                &fields,
                &kind,
                span.clone(),
            )?;
            Ok(GqlBoundPropertyIndexStatement {
                target_kind: GqlPropertyIndexTargetKind::Edge,
                label: label.name,
                fields,
                kind,
                span,
            })
        }
    }
}

fn bind_property_index_fields(
    target_kind: GqlPropertyIndexTargetKind,
    target_variable: &crate::gql::ast::Ident,
    fields: Vec<GqlPropertyIndexField>,
) -> Result<Vec<SecondaryIndexField>, EngineError> {
    fields
        .into_iter()
        .map(|field| bind_property_index_field(target_kind, target_variable, field))
        .collect()
}

fn bind_property_index_field(
    target_kind: GqlPropertyIndexTargetKind,
    target_variable: &crate::gql::ast::Ident,
    field: GqlPropertyIndexField,
) -> Result<SecondaryIndexField, EngineError> {
    validate_on_variable(
        &target_variable.name,
        &field.variable().name,
        &field.variable().span,
    )?;
    match field {
        GqlPropertyIndexField::Property { key, .. } => Ok(SecondaryIndexField::property(key.name)),
        GqlPropertyIndexField::Metadata { function, span, .. } => {
            bind_property_index_metadata_field(target_kind, function, span)
        }
        GqlPropertyIndexField::EndpointId { endpoint, span, .. } => {
            if target_kind != GqlPropertyIndexTargetKind::Edge {
                return Err(gql_index_semantic_error(
                    "GQL index DDL error: endpoint metadata functions are valid only for edge property indexes",
                    span,
                ));
            }
            Ok(match endpoint {
                GqlPropertyIndexEndpointFunction::StartNode => {
                    SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From)
                }
                GqlPropertyIndexEndpointFunction::EndNode => {
                    SecondaryIndexField::edge_meta(EdgeMetadataIndexField::To)
                }
            })
        }
    }
}

fn bind_property_index_metadata_field(
    target_kind: GqlPropertyIndexTargetKind,
    function: GqlPropertyIndexMetadataFunction,
    span: SourceSpan,
) -> Result<SecondaryIndexField, EngineError> {
    match (target_kind, function) {
        (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::Id) => {
            Ok(SecondaryIndexField::node_meta(NodeMetadataIndexField::Id))
        }
        (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::ElementKey) => {
            Ok(SecondaryIndexField::node_meta(NodeMetadataIndexField::Key))
        }
        (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::Weight) => Ok(
            SecondaryIndexField::node_meta(NodeMetadataIndexField::Weight),
        ),
        (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::CreatedAt) => Ok(
            SecondaryIndexField::node_meta(NodeMetadataIndexField::CreatedAt),
        ),
        (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::UpdatedAt) => Ok(
            SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
        ),
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::Id) => {
            Ok(SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Id))
        }
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::Weight) => Ok(
            SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
        ),
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::CreatedAt) => Ok(
            SecondaryIndexField::edge_meta(EdgeMetadataIndexField::CreatedAt),
        ),
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::UpdatedAt) => Ok(
            SecondaryIndexField::edge_meta(EdgeMetadataIndexField::UpdatedAt),
        ),
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::ValidFrom) => Ok(
            SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidFrom),
        ),
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::ValidTo) => Ok(
            SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidTo),
        ),
        (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::ValidFrom)
        | (GqlPropertyIndexTargetKind::Node, GqlPropertyIndexMetadataFunction::ValidTo) => {
            Err(gql_index_semantic_error(
                "GQL index DDL error: validity metadata is valid only for edge property indexes",
                span,
            ))
        }
        (GqlPropertyIndexTargetKind::Edge, GqlPropertyIndexMetadataFunction::ElementKey) => {
            Err(gql_index_semantic_error(
                "GQL index DDL error: elementKey metadata is valid only for node property indexes",
                span,
            ))
        }
    }
}

fn validate_bound_index_spec(
    target_kind: GqlPropertyIndexTargetKind,
    fields: &[SecondaryIndexField],
    kind: &SecondaryIndexKind,
    span: SourceSpan,
) -> Result<(), EngineError> {
    let spec = SecondaryIndexSpec {
        fields: fields.to_vec(),
        kind: kind.clone(),
    };
    let result = match target_kind {
        GqlPropertyIndexTargetKind::Node => spec.validate_for_node(),
        GqlPropertyIndexTargetKind::Edge => spec.validate_for_edge(),
    };
    result.map_err(|err| gql_index_validation_error(err, span))
}

fn gql_index_validation_error(err: EngineError, span: SourceSpan) -> EngineError {
    match err {
        EngineError::InvalidOperation(message) => {
            let message = message
                .strip_prefix("invalid secondary index: ")
                .unwrap_or(&message);
            gql_index_semantic_error(format!("GQL index DDL error: {message}"), span)
        }
        other => other,
    }
}

fn validate_index_label(name: &str, span: &SourceSpan) -> Result<(), EngineError> {
    validate_label_token_name(name).map_err(|err| match err {
        EngineError::InvalidOperation(message) => {
            gql_index_semantic_error(format!("GQL index DDL error: {message}"), span.clone())
        }
        other => other,
    })
}

fn validate_on_variable(
    target_variable: &str,
    on_variable: &str,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    if target_variable != on_variable {
        return Err(gql_index_semantic_error(
            format!(
                "GQL index DDL error: index ON variable '{on_variable}' does not match target variable '{target_variable}'"
            ),
            span.clone(),
        ));
    }
    Ok(())
}

fn gql_index_semantic_error(message: impl Into<String>, span: SourceSpan) -> EngineError {
    EngineError::GqlSemantic {
        code: GqlSemanticErrorCode::InvalidParameter,
        message: message.into(),
        span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::ast::{
        GqlCreatePropertyIndexStatement, GqlPropertyIndexField, GqlPropertyIndexMetadataFunction,
        GqlPropertyIndexTarget, GqlStatementBody, Ident,
    };
    use crate::gql::params::referenced_param_names_for_query;
    use crate::gql::parser::{parse_statement, GqlParseOptions};
    use crate::types::{GqlExecutionOptions, GqlStatementKind};

    fn parse_index(source: &str) -> GqlIndexStatement {
        let statement =
            parse_statement(source, &GqlParseOptions::default()).unwrap_or_else(|err| {
                panic!("expected index statement to parse, got {err:?}");
            });
        assert_eq!(statement.kind, GqlStatementKind::Index);
        let GqlStatementBody::Index(index) = statement.body else {
            panic!("expected index statement body");
        };
        index
    }

    fn bind_property(source: &str) -> GqlBoundPropertyIndexStatement {
        match bind_index_statement(parse_index(source)).expect("index bind should succeed") {
            GqlIndexSemanticPlan::Create(bound) | GqlIndexSemanticPlan::Drop(bound) => bound,
            other => panic!("expected bound property index statement, got {other:?}"),
        }
    }

    #[test]
    fn binds_node_and_edge_property_index_targets_and_kinds() {
        let node_eq =
            bind_property("CREATE PROPERTY INDEX FOR (n:Person) ON (n.status) KIND EQUALITY");
        assert_eq!(node_eq.target_kind, GqlPropertyIndexTargetKind::Node);
        assert_eq!(gql_index_target_kind_name(node_eq.target_kind), "node");
        assert_eq!(node_eq.label, "Person");
        assert_eq!(
            node_eq.fields,
            vec![SecondaryIndexField::property("status")]
        );
        assert_eq!(node_eq.kind, SecondaryIndexKind::Equality);

        let node_range =
            bind_property("CREATE PROPERTY INDEX FOR (n:Person) ON (n.score) KIND RANGE");
        assert_eq!(node_range.target_kind, GqlPropertyIndexTargetKind::Node);
        assert_eq!(node_range.kind, SecondaryIndexKind::Range);

        let edge_eq =
            bind_property("DROP PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.role) KIND EQUALITY");
        assert_eq!(edge_eq.target_kind, GqlPropertyIndexTargetKind::Edge);
        assert_eq!(gql_index_target_kind_name(edge_eq.target_kind), "edge");
        assert_eq!(edge_eq.label, "WORKS_AT");
        assert_eq!(edge_eq.fields, vec![SecondaryIndexField::property("role")]);
        assert_eq!(edge_eq.kind, SecondaryIndexKind::Equality);

        let edge_range =
            bind_property("DROP PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.score) KIND RANGE");
        assert_eq!(edge_range.target_kind, GqlPropertyIndexTargetKind::Edge);
        assert_eq!(edge_range.kind, SecondaryIndexKind::Range);
    }

    #[test]
    fn binder_preserves_unescaped_quoted_label_and_property_names() {
        let node = bind_property(
            "CREATE PROPERTY INDEX FOR (n:\"Display\\\"Label\") ON (n.\"external\\\"id\") KIND EQUALITY",
        );
        assert_eq!(node.label, "Display\"Label");
        assert_eq!(
            node.fields,
            vec![SecondaryIndexField::property("external\"id")]
        );

        let edge = bind_property(
            "CREATE PROPERTY INDEX FOR ()-[r:\"WORKED WITH\"]-() ON (r.\"since-ms\") KIND RANGE",
        );
        assert_eq!(edge.label, "WORKED WITH");
        assert_eq!(edge.fields, vec![SecondaryIndexField::property("since-ms")]);
    }

    #[test]
    fn binder_binds_property_and_metadata_fields_to_native_specs() {
        let node = bind_property(
            "CREATE PROPERTY INDEX FOR (n:Person) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
        );
        assert_eq!(
            node.fields,
            vec![
                SecondaryIndexField::property("tenant_id"),
                SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
            ]
        );
        assert_eq!(node.kind, SecondaryIndexKind::Range);

        let edge = bind_property(
            "CREATE PROPERTY INDEX FOR ()-[r:WORKS_ON]-() ON (r.status, id(startNode(r)), validTo(r)) KIND RANGE",
        );
        assert_eq!(
            edge.fields,
            vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidTo),
            ]
        );
    }

    #[test]
    fn ddl_metadata_function_names_match_case_insensitively() {
        let node = bind_property("CREATE PROPERTY INDEX FOR (n:Person) ON (UPDATEDAT(n)) KIND RANGE");
        assert_eq!(
            node.fields,
            vec![SecondaryIndexField::node_meta(
                NodeMetadataIndexField::UpdatedAt
            )]
        );

        let edge = bind_property(
            "CREATE PROPERTY INDEX FOR ()-[r:WORKS_ON]-() ON (validto(r), ID(STARTNODE(r))) KIND RANGE",
        );
        assert_eq!(
            edge.fields,
            vec![
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidTo),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
            ]
        );
    }

    #[test]
    fn binder_allows_property_metadata_name_and_metadata_function_together() {
        let node = bind_property(
            "CREATE PROPERTY INDEX FOR (n:Person) ON (n.updated_at, updatedAt(n)) KIND EQUALITY",
        );
        assert_eq!(
            node.fields,
            vec![
                SecondaryIndexField::property("updated_at"),
                SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
            ]
        );
    }

    #[test]
    fn binder_validates_labels_through_existing_label_path() {
        let err = bind_index_statement(parse_index(
            "CREATE PROPERTY INDEX FOR (n:\"\") ON (n.status) KIND EQUALITY",
        ))
        .expect_err("empty label should fail semantic validation");
        match err {
            EngineError::GqlSemantic { message, .. } => {
                assert!(message.contains("label token name must not be empty"));
            }
            other => panic!("expected GQL semantic label error, got {other:?}"),
        }
    }

    #[test]
    fn binder_returns_empty_referenced_params_for_valid_index_ddl() {
        for source in [
            "CREATE PROPERTY INDEX FOR (n:Person) ON (n.status) KIND EQUALITY",
            "CREATE PROPERTY INDEX FOR (n:Person) ON (n.status, updatedAt(n)) KIND RANGE",
            "DROP PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.score) KIND RANGE",
            "DROP PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.status, validTo(r)) KIND RANGE",
            "SHOW PROPERTY INDEXES",
        ] {
            let params =
                referenced_param_names_for_query(source, &GqlExecutionOptions::default()).unwrap();
            assert!(params.is_empty(), "source: {source}");
        }
    }

    #[test]
    fn binder_rejects_on_variable_mismatch_at_on_variable_span() {
        let index = parse_index("CREATE PROPERTY INDEX FOR (n:Person) ON (m.status) KIND EQUALITY");
        let expected_span = match &index {
            GqlIndexStatement::Create(create) => match &create.target {
                GqlPropertyIndexTarget::Node { fields, .. } => fields[0].variable().span.clone(),
                other => panic!("expected node index target, got {other:?}"),
            },
            other => panic!("expected create index statement, got {other:?}"),
        };
        let err = bind_index_statement(index).expect_err("variable mismatch should fail");
        match err {
            EngineError::GqlSemantic { message, span, .. } => {
                assert_eq!(
                    message,
                    "GQL index DDL error: index ON variable 'm' does not match target variable 'n'"
                );
                assert_eq!(span, expected_span);
            }
            other => panic!("expected GQL semantic variable mismatch, got {other:?}"),
        }
    }

    #[test]
    fn binder_binds_show_scope() {
        let plan = bind_index_statement(parse_index("SHOW EDGE PROPERTY INDEXES"))
            .expect("show should bind");
        match plan {
            GqlIndexSemanticPlan::Show { scope, .. } => {
                assert_eq!(scope, GqlShowPropertyIndexScope::Edge);
            }
            other => panic!("expected show semantic plan, got {other:?}"),
        }
    }

    #[test]
    fn binder_rejects_duplicate_and_wrong_target_fields_through_native_validation() {
        let duplicate = bind_index_statement(parse_index(
            "CREATE PROPERTY INDEX FOR (n:Person) ON (n.status, n.status) KIND EQUALITY",
        ))
        .expect_err("duplicate fields should fail");
        match duplicate {
            EngineError::GqlSemantic { message, .. } => {
                assert!(message.contains("GQL index DDL error: duplicate field property `status`"));
            }
            other => panic!("expected duplicate-field GQL semantic error, got {other:?}"),
        }

        let span = SourceSpan::new(0, 1, 1, 1);
        let node_with_edge_metadata = GqlIndexStatement::Create(GqlCreatePropertyIndexStatement {
            target: GqlPropertyIndexTarget::Node {
                variable: Ident {
                    name: "n".to_string(),
                    span: span.clone(),
                },
                label: crate::gql::ast::GqlIndexName {
                    name: "Person".to_string(),
                    span: span.clone(),
                },
                fields: vec![GqlPropertyIndexField::Metadata {
                    function: GqlPropertyIndexMetadataFunction::ValidTo,
                    function_span: span.clone(),
                    variable: Ident {
                        name: "n".to_string(),
                        span: span.clone(),
                    },
                    span: span.clone(),
                }],
                field_list_span: span.clone(),
                span: span.clone(),
            },
            kind: GqlPropertyIndexKind::Equality,
            kind_span: span.clone(),
            span: span.clone(),
        });
        let err = bind_index_statement(node_with_edge_metadata)
            .expect_err("node target should reject edge metadata");
        match err {
            EngineError::GqlSemantic { message, .. } => {
                assert!(message.contains("validity metadata is valid only for edge"));
            }
            other => panic!("expected wrong-target GQL semantic error, got {other:?}"),
        }

        let edge_with_node_metadata = GqlIndexStatement::Create(GqlCreatePropertyIndexStatement {
            target: GqlPropertyIndexTarget::Edge {
                variable: Ident {
                    name: "r".to_string(),
                    span: span.clone(),
                },
                label: crate::gql::ast::GqlIndexName {
                    name: "WORKS_AT".to_string(),
                    span: span.clone(),
                },
                fields: vec![GqlPropertyIndexField::Metadata {
                    function: GqlPropertyIndexMetadataFunction::ElementKey,
                    function_span: span.clone(),
                    variable: Ident {
                        name: "r".to_string(),
                        span: span.clone(),
                    },
                    span: span.clone(),
                }],
                field_list_span: span.clone(),
                span: span.clone(),
            },
            kind: GqlPropertyIndexKind::Equality,
            kind_span: span.clone(),
            span,
        });
        let err = bind_index_statement(edge_with_node_metadata)
            .expect_err("edge target should reject node metadata");
        match err {
            EngineError::GqlSemantic { message, .. } => {
                assert!(message.contains("elementKey metadata is valid only for node"));
            }
            other => panic!("expected wrong-target GQL semantic error, got {other:?}"),
        }
    }
}
