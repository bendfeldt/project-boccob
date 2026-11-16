# Column masks and row filters, driven by the classification generated from dbt meta
# (data.lakehouse.classification, see governance/classification).
package trino

import data.lakehouse.classification
import data.lakehouse.config

table_key(t) := sprintf("%s.%s.%s", [t.catalogName, t.schemaName, t.tableName])

# classification of one column: generated metadata first, catalog level defaults second
column_class(col) := c if {
	c := classification.tables[table_key(col)].columns[col.columnName]
} else := c if {
	c := config.catalogs[col.catalogName].column_defaults[col.columnName]
} else := null

unclassified_policy(col) := p if {
	p := schema_config(col.catalogName, col.schemaName).unclassified
} else := p if {
	p := config.catalogs[col.catalogName].unclassified
} else := "allow"

needs_mask(col) if {
	not can_see_pii
	c := column_class(col)
	c != null
	c.pii in config.pii_levels_masked
}

needs_mask(col) if {
	not can_see_pii
	column_class(col) == null
	col.catalogName in object.keys(config.catalogs)
	col.schemaName != "information_schema" # catalog metadata, already filtered per object
	unclassified_policy(col) == "mask"
}

mask_strategy(col) := s if {
	s := column_class(col).mask
} else := "null"

is_text(col) if startswith(lower(object.get(col, "columnType", "")), "varchar")

quoted(name) := sprintf("\"%s\"", [replace(name, "\"", "\"\"")])

# SQL expression that replaces the column value; must be coercible to the column type
mask_expression(col) := expr if {
	is_text(col)
	mask_strategy(col) == "redact"
	expr := sprintf("CASE WHEN %s IS NULL THEN NULL ELSE '*****' END", [quoted(col.columnName)])
} else := expr if {
	is_text(col)
	mask_strategy(col) == "email"
	expr := sprintf("regexp_replace(%s, '^[^@]+', '*****')", [quoted(col.columnName)])
} else := expr if {
	is_text(col)
	mask_strategy(col) == "hash"
	expr := sprintf("to_hex(sha256(to_utf8(%s)))", [quoted(col.columnName)])
} else := "NULL"

# single column endpoint
columnMask := {"expression": mask_expression(col)} if {
	col := input.action.resource.column
	needs_mask(col)
}

# batch endpoint
batchColumnMasks contains {"index": i, "viewExpression": {"expression": mask_expression(col)}} if {
	some i, res in input.action.filterResources
	col := res.column
	needs_mask(col)
}

# row filters declared on a model (meta.row_filter), unless the user is in an exempt group
rowFilters contains {"expression": f.expression} if {
	t := input.action.resource.table
	f := classification.tables[table_key(t)].row_filter
	count(groups & {g | some g in object.get(f, "exempt_groups", [])}) == 0
}
