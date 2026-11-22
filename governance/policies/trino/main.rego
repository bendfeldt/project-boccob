# Trino access control for the lakehouse (Trino OPA plugin).
#
# Endpoints (see docs/platform-contract.md):
#   opa.policy.uri                       /v1/data/trino/allow
#   opa.policy.batched-uri               /v1/data/trino/batch
#   opa.policy.row-filters-uri           /v1/data/trino/rowFilters
#   opa.policy.batch-column-masking-uri  /v1/data/trino/batchColumnMasks
#   (opa.policy.column-masking-uri       /v1/data/trino/columnMask - alternative to the batch one)
#
# Model: deny by default. Groups (Keycloak) grant layer access; the pii group removes column
# masks. Column classification and row filters are data generated from the dbt project.
package trino

import data.lakehouse.config

default allow := false

identity := input.context.identity

operation := input.action.operation

resource := object.get(input.action, "resource", {})

# --- identity -----------------------------------------------------------------------------

# groups come only from Trino's group provider (Keycloak groups, static service account entries)
groups contains g if some g in object.get(identity, "groups", [])

is_admin if config.admin_group in groups

is_known if count(groups & {g | some g in config.known_groups}) > 0

can_see_pii if config.pii_group in groups

# --- catalog / schema permissions ---------------------------------------------------------

schema_config(catalog, schema) := object.get(object.get(config.catalogs[catalog], "schemas", {}), schema, {})

# groups allowed for an access mode on a schema: the schema override wins over the catalog
allowed_groups(catalog, schema, mode) := gs if {
	gs := schema_config(catalog, schema)[mode]
} else := gs if {
	gs := config.catalogs[catalog][mode]
} else := []

catalog_access(catalog, _) if {
	is_admin
	catalog in object.keys(config.catalogs)
}

catalog_access(catalog, mode) if {
	some g in config.catalogs[catalog][mode]
	g in groups
}

# reading catalog level metadata is also allowed when any schema override grants read
catalog_access(catalog, "read") if {
	some _, override in object.get(config.catalogs[catalog], "schemas", {})
	some g in object.get(override, "read", [])
	g in groups
}

schema_access(catalog, schema, mode) if {
	is_admin
	catalog in object.keys(config.catalogs)
	not protected_write(catalog, schema, mode)
}

# protected schemas: writes only through their own write list (no admin bypass)
protected_write(catalog, schema, "write") if schema_config(catalog, schema).protected == true

is_service if config.service_group in groups

schema_access(catalog, schema, mode) if {
	some g in allowed_groups(catalog, schema, mode)
	g in groups
}

# information_schema follows catalog read access (Trino filters its content per object)
schema_access(catalog, "information_schema", "read") if catalog_access(catalog, "read")

# --- operation classes --------------------------------------------------------------------

catalog_read_ops := {"AccessCatalog", "ShowSchemas", "FilterCatalogs"}

schema_read_ops := {"ShowTables", "FilterSchemas", "ShowCreateSchema", "ShowFunctions"}

table_read_ops := {"SelectFromColumns", "ShowColumns", "FilterTables", "FilterColumns", "ShowCreateTable"}

schema_write_ops := {"CreateSchema", "DropSchema", "RenameSchema"}

table_write_ops := {
	"CreateTable", "DropTable", "RenameTable", "SetTableProperties", "SetTableComment",
	"SetColumnComment", "AddColumn", "AlterColumn", "DropColumn", "RenameColumn",
	"InsertIntoTable", "DeleteFromTable", "TruncateTable", "UpdateTableColumns",
	"CreateView", "DropView", "RenameView", "SetViewComment",
	"CreateMaterializedView", "DropMaterializedView", "RenameMaterializedView",
	"RefreshMaterializedView", "SetMaterializedViewProperties", "SetMaterializedViewComment",
	"ExecuteTableProcedure",
}

# session properties that weaken retention safety nets are reserved for writers
function_ops := {"ExecuteFunction", "FilterFunctions"}

# --- allow ----------------------------------------------------------------------------------

# every known principal may submit queries and see/kill its own
allow if {
	operation == "ExecuteQuery"
	is_known
}

allow if {
	operation in {"ViewQueryOwnedBy", "FilterViewQueryOwnedBy", "KillQueryOwnedBy"}
	is_known
	resource.user.user == identity.user
}

allow if {
	operation in {"ViewQueryOwnedBy", "FilterViewQueryOwnedBy"}
	some g in config.query_observers
	g in groups
}

allow if {
	operation in {"KillQueryOwnedBy", "ReadSystemInformation", "WriteSystemInformation"}
	is_admin
}

allow if {
	operation == "ReadSystemInformation"
	some g in config.query_observers
	g in groups
}

allow if {
	operation == "SetSystemSessionProperty"
	is_known
}

allow if {
	operation == "SetCatalogSessionProperty"
	catalog_access(resource.catalogSessionProperty.catalogName, "read")
	not retention_override(resource.catalogSessionProperty.propertyName)
}

# lowering the snapshot / orphan file retention floor makes deleted data unrecoverable at once:
# reserved for the pipeline service accounts (maintenance and erasure jobs)
allow if {
	operation == "SetCatalogSessionProperty"
	retention_override(resource.catalogSessionProperty.propertyName)
	is_service
	resource.catalogSessionProperty.catalogName in object.keys(config.catalogs)
}

retention_override(name) if name in {"expire_snapshots_min_retention", "remove_orphan_files_min_retention"}

# built-in functions are harmless; user defined functions need write access to their schema
allow if {
	operation in function_ops
	is_known
}

allow if {
	operation in {"CreateFunction", "DropFunction", "ShowCreateFunction"}
	schema_access(resource.function.catalogName, resource.function.schemaName, "write")
}

allow if {
	operation == "ExecuteProcedure"
	schema_access(resource.function.catalogName, resource.function.schemaName, "write")
}

# the system catalog: metadata for clients (jdbc, metadata), runtime only for observers
allow if {
	is_known
	system_metadata_access
}

system_metadata_access if {
	operation in catalog_read_ops
	resource.catalog.name == "system"
}

system_metadata_access if {
	operation in schema_read_ops
	resource.schema.catalogName == "system"
}

system_metadata_access if {
	operation in table_read_ops
	resource.table.catalogName == "system"
	resource.table.schemaName in {"jdbc", "metadata", "information_schema"}
}

allow if {
	operation in table_read_ops
	resource.table.catalogName == "system"
	some g in config.query_observers
	g in groups
}

# lakehouse catalogs
allow if {
	operation in catalog_read_ops
	catalog_access(resource.catalog.name, "read")
}

allow if {
	operation in schema_read_ops
	schema_access(resource.schema.catalogName, resource.schema.schemaName, "read")
}

allow if {
	operation in table_read_ops
	schema_access(resource.table.catalogName, resource.table.schemaName, "read")
}

allow if {
	operation in schema_write_ops
	schema_access(resource.schema.catalogName, resource.schema.schemaName, "write")
	target_schema_writable
}

allow if {
	operation in table_write_ops
	schema_access(resource.table.catalogName, resource.table.schemaName, "write")
	target_table_writable
}

# CREATE VIEW ... AS SELECT needs read access on what the view selects from
allow if {
	operation in {"CreateViewWithSelectFromColumns", "CreateViewWithExecuteFunction"}
	schema_access(resource.table.catalogName, resource.table.schemaName, "read")
}

allow if {
	operation == "CreateViewWithExecuteFunction"
	is_known
	resource.function
}

# renames must stay inside a writable schema
target_schema_writable if not input.action.targetResource

target_schema_writable if {
	t := input.action.targetResource.schema
	schema_access(t.catalogName, t.schemaName, "write")
}

target_table_writable if not input.action.targetResource

target_table_writable if {
	t := input.action.targetResource.table
	schema_access(t.catalogName, t.schemaName, "write")
}

# Not allowed for anyone through this policy (no rule): ImpersonateUser, CreateCatalog,
# DropCatalog, Set*Authorization. Grant/revoke/role management is disabled in the plugin.

# --- batch (filter) endpoint ----------------------------------------------------------------

batch contains i if {
	some i, raw_resource in input.action.filterResources
	allow with input.action.resource as raw_resource
}

# FilterColumns sends one table with many columns: answer per column index
batch contains i if {
	operation == "FilterColumns"
	count(input.action.filterResources) == 1
	raw_resource := input.action.filterResources[0]
	count(raw_resource.table.columns) > 0
	some i, _ in raw_resource.table.columns
	allow with input.action.resource as raw_resource
}
