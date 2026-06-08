use crate::error::EngineError;
use crate::gql::ast::{
    GqlIndexStatement, GqlPropertyIndexKind, GqlPropertyIndexTarget, GqlShowPropertyIndexScope,
};
use crate::types::{
    validate_label_token_name, GqlSemanticErrorCode, SecondaryIndexKind, SourceSpan,
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
    pub(crate) prop_key: String,
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
            on_variable,
            prop_key,
            ..
        } => {
            validate_index_label(&label.name, &label.span)?;
            validate_on_variable(&variable.name, &on_variable.name, &on_variable.span)?;
            Ok(GqlBoundPropertyIndexStatement {
                target_kind: GqlPropertyIndexTargetKind::Node,
                label: label.name,
                prop_key: prop_key.name,
                kind,
                span,
            })
        }
        GqlPropertyIndexTarget::Edge {
            variable,
            label,
            on_variable,
            prop_key,
            ..
        } => {
            validate_index_label(&label.name, &label.span)?;
            validate_on_variable(&variable.name, &on_variable.name, &on_variable.span)?;
            Ok(GqlBoundPropertyIndexStatement {
                target_kind: GqlPropertyIndexTargetKind::Edge,
                label: label.name,
                prop_key: prop_key.name,
                kind,
                span,
            })
        }
    }
}

fn validate_index_label(name: &str, span: &SourceSpan) -> Result<(), EngineError> {
    validate_label_token_name(name).map_err(|err| match err {
        EngineError::InvalidOperation(message) => gql_index_semantic_error(message, span.clone()),
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
                "index ON variable '{on_variable}' does not match target variable '{target_variable}'"
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
    use crate::gql::ast::{GqlPropertyIndexTarget, GqlStatementBody};
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
        assert_eq!(node_eq.prop_key, "status");
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
        assert_eq!(edge_eq.prop_key, "role");
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
        assert_eq!(node.prop_key, "external\"id");

        let edge = bind_property(
            "CREATE PROPERTY INDEX FOR ()-[r:\"WORKED WITH\"]-() ON (r.\"since-ms\") KIND RANGE",
        );
        assert_eq!(edge.label, "WORKED WITH");
        assert_eq!(edge.prop_key, "since-ms");
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
            "DROP PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.score) KIND RANGE",
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
                GqlPropertyIndexTarget::Node { on_variable, .. } => on_variable.span.clone(),
                other => panic!("expected node index target, got {other:?}"),
            },
            other => panic!("expected create index statement, got {other:?}"),
        };
        let err = bind_index_statement(index).expect_err("variable mismatch should fail");
        match err {
            EngineError::GqlSemantic { message, span, .. } => {
                assert_eq!(
                    message,
                    "index ON variable 'm' does not match target variable 'n'"
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
}
