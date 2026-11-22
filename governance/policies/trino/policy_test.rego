package trino_test

import data.trino

# --- fixtures -------------------------------------------------------------------------------

classification := {"tables": {
	"clean.borgerportal.citizens": {"columns": {
		"citizen_id": {"pii": "indirect", "pseudonymised": true},
		"cpr_number": {"pii": "direct", "cpr": true, "mask": "redact"},
		"email": {"pii": "direct", "mask": "email"},
		"birth_date": {"pii": "direct", "mask": "null"},
		"postal_code": {"pii": "indirect"},
		"is_deleted": {"pii": "none"},
	}},
	"curated.borgerservice.dim_citizen": {"columns": {
		"citizen_key": {"pii": "none"},
		"full_name": {"pii": "direct", "mask": "redact"},
	}},
	"curated.borgerservice.dp_payment_summary_v1": {
		"columns": {"total_amount": {"pii": "none"}},
		"row_filter": {"expression": "is_small_cell = false", "exempt_groups": ["data-engineer", "pii-reader", "platform-admin"]},
	},
}}

ctx(user, groups) := {"identity": {"user": user, "groups": groups}, "softwareStack": {"trinoVersion": "484"}}

table_input(user, groups, op, catalog, schema, table) := {
	"context": ctx(user, groups),
	"action": {"operation": op, "resource": {"table": {"catalogName": catalog, "schemaName": schema, "tableName": table, "columns": ["a"]}}},
}

column_input(user, groups, catalog, schema, table, column, type) := {
	"context": ctx(user, groups),
	"action": {"operation": "GetColumnMask", "resource": {"column": {
		"catalogName": catalog, "schemaName": schema, "tableName": table, "columnName": column, "columnType": type,
	}}},
}

allowed(inp) if trino.allow with input as inp with data.lakehouse.classification as classification

mask(inp) := m if {
	m := trino.columnMask with input as inp with data.lakehouse.classification as classification
}

# --- baseline -------------------------------------------------------------------------------

test_unknown_user_cannot_execute_query if {
	not allowed({"context": ctx("mallory", []), "action": {"operation": "ExecuteQuery"}})
}

test_unknown_group_is_denied if {
	not allowed({"context": ctx("mallory", ["marketing"]), "action": {"operation": "ExecuteQuery"}})
}

test_every_role_can_execute_query if {
	every g in ["platform-admin", "data-engineer", "analyst", "auditor"] {
		allowed({"context": ctx("u", [g]), "action": {"operation": "ExecuteQuery"}})
	}
}

test_nobody_can_impersonate if {
	every g in ["platform-admin", "data-engineer", "analyst", "auditor", "pii-reader"] {
		not allowed({"context": ctx("u", [g]), "action": {"operation": "ImpersonateUser", "resource": {"user": {"user": "alice"}}}})
	}
}

# --- layer access ---------------------------------------------------------------------------

test_analyst_reads_curated_only if {
	allowed(table_input("anna", ["analyst"], "SelectFromColumns", "curated", "borgerservice", "dim_citizen"))
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "consolidated", "party", "person"))
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "clean", "borgerportal", "citizens"))
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "raw", "borgerportal", "citizens"))
}

test_analyst_cannot_read_keystore_or_audit if {
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "curated", "keystore", "dim_citizen"))
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "curated", "audit", "erasure_log"))
}

test_analyst_cannot_write if {
	every op in ["InsertIntoTable", "DeleteFromTable", "CreateTable", "DropTable", "UpdateTableColumns", "ExecuteTableProcedure"] {
		not allowed(table_input("anna", ["analyst"], op, "curated", "borgerservice", "dim_citizen"))
	}
}

test_analyst_cannot_access_raw_catalog if {
	not allowed({"context": ctx("anna", ["analyst"]), "action": {"operation": "AccessCatalog", "resource": {"catalog": {"name": "raw"}}}})
	allowed({"context": ctx("anna", ["analyst"]), "action": {"operation": "AccessCatalog", "resource": {"catalog": {"name": "curated"}}}})
}

test_engineer_reads_and_writes_all_layers if {
	every c in ["raw", "clean", "consolidated", "curated"] {
		allowed(table_input("erik", ["data-engineer"], "SelectFromColumns", c, "s", "t"))
		allowed(table_input("erik", ["data-engineer"], "InsertIntoTable", c, "s", "t"))
		allowed(table_input("erik", ["data-engineer"], "ExecuteTableProcedure", c, "s", "t"))
	}
}

test_engineer_cannot_touch_unmanaged_catalog if {
	not allowed(table_input("erik", ["data-engineer"], "SelectFromColumns", "tpch", "tiny", "nation"))
}

test_auditor_is_read_only_everywhere if {
	every c in ["raw", "clean", "consolidated", "curated"] {
		allowed(table_input("axel", ["auditor"], "SelectFromColumns", c, "s", "t"))
		not allowed(table_input("axel", ["auditor"], "InsertIntoTable", c, "s", "t"))
		not allowed(table_input("axel", ["auditor"], "DeleteFromTable", c, "s", "t"))
	}
	allowed(table_input("axel", ["auditor"], "SelectFromColumns", "curated", "audit", "erasure_log"))
}

test_pii_reader_alone_grants_no_data_access if {
	not allowed(table_input("p", ["pii-reader"], "SelectFromColumns", "curated", "borgerservice", "dim_citizen"))
}

test_admin_full_access_to_layers if {
	allowed(table_input("alice", ["platform-admin"], "DropTable", "raw", "borgerportal", "citizens"))
	allowed(table_input("alice", ["platform-admin"], "SelectFromColumns", "curated", "keystore", "dim_citizen"))
}

test_rename_must_stay_in_writable_schema if {
	inp := object.union(
		table_input("erik", ["data-engineer"], "RenameTable", "curated", "borgerservice", "t"),
		{"action": {"targetResource": {"table": {"catalogName": "tpch", "schemaName": "x", "tableName": "t"}}}},
	)
	not allowed(inp)
}

# --- queries and system catalog ------------------------------------------------------------

test_user_sees_own_queries_only if {
	allowed({"context": ctx("anna", ["analyst"]), "action": {"operation": "ViewQueryOwnedBy", "resource": {"user": {"user": "anna"}}}})
	not allowed({"context": ctx("anna", ["analyst"]), "action": {"operation": "ViewQueryOwnedBy", "resource": {"user": {"user": "erik"}}}})
}

test_auditor_sees_all_queries_but_cannot_kill if {
	allowed({"context": ctx("axel", ["auditor"]), "action": {"operation": "ViewQueryOwnedBy", "resource": {"user": {"user": "erik"}}}})
	not allowed({"context": ctx("axel", ["auditor"]), "action": {"operation": "KillQueryOwnedBy", "resource": {"user": {"user": "erik"}}}})
}

test_system_runtime_restricted if {
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "system", "runtime", "queries"))
	allowed(table_input("axel", ["auditor"], "SelectFromColumns", "system", "runtime", "queries"))
	allowed(table_input("anna", ["analyst"], "SelectFromColumns", "system", "jdbc", "tables"))
}

prop(user, groups, catalog, name) := {"context": ctx(user, groups), "action": {"operation": "SetCatalogSessionProperty", "resource": {"catalogSessionProperty": {"catalogName": catalog, "propertyName": name}}}}

test_retention_floor_only_for_service_accounts if {
	not allowed(prop("anna", ["analyst"], "curated", "expire_snapshots_min_retention"))
	allowed(prop("anna", ["analyst"], "curated", "projection_pushdown_enabled"))
	not allowed(prop("erik", ["data-engineer"], "curated", "expire_snapshots_min_retention"))
	not allowed(prop("erik", ["data-engineer"], "raw", "remove_orphan_files_min_retention"))
	not allowed(prop("alice", ["platform-admin"], "curated", "expire_snapshots_min_retention"))
	allowed(prop("erik", ["data-engineer"], "curated", "projection_pushdown_enabled"))
	allowed(prop("svc-dagster", ["data-engineer", "pii-reader", "lakehouse-service"], "curated", "expire_snapshots_min_retention"))
	not allowed(prop("svc-dagster", ["lakehouse-service"], "tpch", "expire_snapshots_min_retention"))
}

svc := ["data-engineer", "pii-reader", "lakehouse-service"]

protected_tables := [
	["curated", "audit", "erasure_log"],
	["curated", "keystore", "dim_citizen"],
	["raw", "governance", "erasure_requests"],
]

write_ops := ["InsertIntoTable", "DeleteFromTable", "UpdateTableColumns", "TruncateTable", "DropTable", "CreateTable", "ExecuteTableProcedure", "AddColumn"]

test_humans_cannot_write_protected_schemas if {
	every t in protected_tables {
		every op in write_ops {
			every g in [["data-engineer"], ["platform-admin"], ["data-engineer", "pii-reader"], ["auditor"]] {
				not allowed(table_input("human", g, op, t[0], t[1], t[2]))
			}
		}
	}
}

test_service_account_writes_protected_schemas if {
	every t in protected_tables {
		every op in write_ops {
			allowed(table_input("svc-dagster", svc, op, t[0], t[1], t[2]))
		}
	}
}

schema_op(user, groups, op, catalog, schema) := {"context": ctx(user, groups), "action": {"operation": op, "resource": {"schema": {"catalogName": catalog, "schemaName": schema}}}}

test_protected_schema_ddl_only_for_service if {
	not allowed(schema_op("erik", ["data-engineer"], "DropSchema", "curated", "audit"))
	not allowed(schema_op("alice", ["platform-admin"], "DropSchema", "curated", "keystore"))
	not allowed(schema_op("erik", ["data-engineer"], "CreateSchema", "raw", "governance"))
	allowed(schema_op("svc-dagster", svc, "CreateSchema", "curated", "audit"))
	allowed(schema_op("erik", ["data-engineer"], "CreateSchema", "curated", "borgerservice"))
}

test_protected_schema_reads if {
	allowed(table_input("axel", ["auditor"], "SelectFromColumns", "curated", "audit", "erasure_log"))
	allowed(table_input("axel", ["auditor"], "SelectFromColumns", "raw", "governance", "erasure_requests"))
	not allowed(table_input("erik", ["data-engineer"], "SelectFromColumns", "curated", "audit", "erasure_log"))
	not allowed(table_input("erik", ["data-engineer"], "SelectFromColumns", "raw", "governance", "erasure_requests"))
	allowed(table_input("erik", ["data-engineer"], "SelectFromColumns", "curated", "keystore", "dim_citizen"))
	allowed(table_input("alice", ["platform-admin"], "SelectFromColumns", "curated", "audit", "erasure_log"))
	not allowed(table_input("anna", ["analyst"], "SelectFromColumns", "curated", "keystore", "dim_citizen"))
}

test_engineer_still_writes_unprotected_schemas if {
	allowed(table_input("erik", ["data-engineer"], "InsertIntoTable", "curated", "borgerservice", "fact_payment"))
	allowed(table_input("erik", ["data-engineer"], "InsertIntoTable", "raw", "borgerportal", "citizens"))
	allowed(table_input("alice", ["platform-admin"], "DropTable", "clean", "borgerportal", "citizens"))
}

# --- column masking -------------------------------------------------------------------------

test_cpr_masked_for_engineer_without_pii_role if {
	mask(column_input("erik", ["data-engineer"], "clean", "borgerportal", "citizens", "cpr_number", "varchar")) == {"expression": "CASE WHEN \"cpr_number\" IS NULL THEN NULL ELSE '*****' END"}
}

test_cpr_visible_for_pii_reader if {
	not mask(column_input("svc-dagster", ["data-engineer", "pii-reader"], "clean", "borgerportal", "citizens", "cpr_number", "varchar"))
}

test_admin_without_pii_role_is_masked if {
	mask(column_input("alice", ["platform-admin"], "clean", "borgerportal", "citizens", "cpr_number", "varchar"))
}

test_email_mask_keeps_domain if {
	mask(column_input("anna", ["analyst"], "clean", "borgerportal", "citizens", "email", "varchar")).expression == "regexp_replace(\"email\", '^[^@]+', '*****')"
}

test_non_text_column_masked_with_null if {
	mask(column_input("anna", ["analyst"], "clean", "borgerportal", "citizens", "birth_date", "date")).expression == "NULL"
}

test_indirect_and_none_not_masked if {
	not mask(column_input("anna", ["analyst"], "clean", "borgerportal", "citizens", "postal_code", "varchar"))
	not mask(column_input("anna", ["analyst"], "clean", "borgerportal", "citizens", "is_deleted", "boolean"))
}

test_unclassified_column_is_masked_in_governed_layers if {
	mask(column_input("anna", ["analyst"], "curated", "borgerservice", "dim_citizen", "new_column", "varchar")).expression == "NULL"
	mask(column_input("erik", ["data-engineer"], "clean", "borgerportal", "brand_new_table", "x", "bigint")).expression == "NULL"
}

test_information_schema_is_never_masked if {
	not mask(column_input("anna", ["analyst"], "curated", "information_schema", "schemata", "schema_name", "varchar"))
}

test_keystore_columns_visible_to_engineers if {
	not mask(column_input("erik", ["data-engineer"], "curated", "keystore", "dim_citizen", "business_key", "varchar"))
}

test_raw_payload_masked_without_pii_role if {
	mask(column_input("erik", ["data-engineer"], "raw", "borgerportal", "anything", "payload", "varchar")).expression == "NULL"
	not mask(column_input("erik", ["data-engineer"], "raw", "borgerportal", "anything", "_load_id", "varchar"))
	not mask(column_input("svc", ["data-engineer", "pii-reader"], "raw", "borgerportal", "anything", "payload", "varchar"))
}

test_batch_column_masks_return_indices if {
	inp := {
		"context": ctx("anna", ["analyst"]),
		"action": {"operation": "GetColumnMask", "filterResources": [
			{"column": {"catalogName": "curated", "schemaName": "borgerservice", "tableName": "dim_citizen", "columnName": "citizen_key", "columnType": "bigint"}},
			{"column": {"catalogName": "curated", "schemaName": "borgerservice", "tableName": "dim_citizen", "columnName": "full_name", "columnType": "varchar"}},
		]},
	}
	result := trino.batchColumnMasks with input as inp with data.lakehouse.classification as classification
	result == {{"index": 1, "viewExpression": {"expression": "CASE WHEN \"full_name\" IS NULL THEN NULL ELSE '*****' END"}}}
}

# --- row filters ----------------------------------------------------------------------------

filters(user, groups) := f if {
	f := trino.rowFilters with input as table_input(user, groups, "GetRowFilters", "curated", "borgerservice", "dp_payment_summary_v1")
		with data.lakehouse.classification as classification
}

test_small_cells_hidden_from_analyst if {
	filters("anna", ["analyst"]) == {{"expression": "is_small_cell = false"}}
}

test_small_cells_visible_to_exempt_groups if {
	count(filters("erik", ["data-engineer"])) == 0
	count(filters("petra", ["analyst", "pii-reader"])) == 0
}

test_no_row_filter_on_other_tables if {
	result := trino.rowFilters with input as table_input("anna", ["analyst"], "GetRowFilters", "curated", "borgerservice", "dim_citizen")
		with data.lakehouse.classification as classification
	count(result) == 0
}

# --- batch filtering ------------------------------------------------------------------------

test_batch_filters_catalogs if {
	inp := {
		"context": ctx("anna", ["analyst"]),
		"action": {"operation": "FilterCatalogs", "filterResources": [
			{"catalog": {"name": "raw"}}, {"catalog": {"name": "curated"}}, {"catalog": {"name": "system"}}, {"catalog": {"name": "clean"}},
		]},
	}
	result := trino.batch with input as inp with data.lakehouse.classification as classification
	result == {1, 2}
}

test_batch_filters_schemas_hides_keystore_from_analyst if {
	inp := {
		"context": ctx("anna", ["analyst"]),
		"action": {"operation": "FilterSchemas", "filterResources": [
			{"schema": {"catalogName": "curated", "schemaName": "borgerservice"}},
			{"schema": {"catalogName": "curated", "schemaName": "keystore"}},
		]},
	}
	result := trino.batch with input as inp with data.lakehouse.classification as classification
	result == {0}
}

test_batch_filter_columns_single_table if {
	inp := {
		"context": ctx("anna", ["analyst"]),
		"action": {"operation": "FilterColumns", "filterResources": [{"table": {
			"catalogName": "curated", "schemaName": "borgerservice", "tableName": "dim_citizen", "columns": ["citizen_key", "full_name"],
		}}]},
	}
	result := trino.batch with input as inp with data.lakehouse.classification as classification
	result == {0, 1}
}

# --- groups come from Trino only -------------------------------------------------------------

test_bundle_data_cannot_grant_groups if {
	inp := table_input("svc-dbt", [], "InsertIntoTable", "clean", "borgerportal", "citizens")
	not trino.allow with input as inp
		with data.lakehouse.classification as classification
		with data.lakehouse.config.principals as {"svc-dbt": {"groups": ["data-engineer", "pii-reader"]}}
}
