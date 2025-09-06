package loader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/kenshaw/snaker"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/xo/dbtpl/models"
	xo "github.com/xo/dbtpl/types"
)

func init() {
	Register("postgres", Loader{
		Mask:               "$%d",
		Flags:              PostgresFlags,
		Schema:             models.PostgresSchema,
		Enums:              models.PostgresEnums,
		EnumValues:         models.PostgresEnumValues,
		CompositeTypes:     models.PostgresCompositeTypes,
		CompositeTypeAttrs: models.PostgresCompositeTypeAttrs,
		Procs:              models.PostgresProcs,
		ProcParams:         models.PostgresProcParams,
		Tables:             models.PostgresTables,
		TableColumns:       PostgresTableColumns,
		TableSequences:     models.PostgresTableSequences,
		TableForeignKeys:   models.PostgresTableForeignKeys,
		TableIndexes:       models.PostgresTableIndexes,
		IndexColumns:       PostgresIndexColumns,
		ViewCreate:         models.PostgresViewCreate,
		ViewSchema:         models.PostgresViewSchema,
		ViewDrop:           models.PostgresViewDrop,
		ViewStrip:          PostgresViewStrip,
	})
}

// PostgresFlags returnss the postgres flags.
func PostgresFlags() []xo.Flag {
	return []xo.Flag{
		{
			ContextKey: oidsKey,
			Type:       "bool",
			Desc:       "enable postgres OIDs",
		},
	}
}

// StdlibPostgresGoType parses a type into a Go type based on the databate type definition.
//
// For array types, it returns the standard go array ([]<type>).
func StdlibPostgresGoType(d xo.Type, schema, itype, _ string) (string, string, error) {
	goType, zero, err := PostgresGoType(d, schema, itype)
	if err != nil {
		return "", "", err
	}
	if d.IsArray {
		arrType, ok := pgStdArrMapping[goType]
		goType, zero = "[]byte", "nil"
		if ok {
			goType = arrType
		}
	}
	return goType, zero, nil
}

// PQPostgresGoType parses a type into a Go type based on the databate type definition.
//
// For array types, it returns the equivalent as defined in `github.com/lib/pq`.
func PQPostgresGoType(d xo.Type, schema, itype, _ string) (string, string, error) {
	goType, zero, err := PostgresGoType(d, schema, itype)
	if err != nil {
		return "", "", err
	}
	if d.IsArray {
		arrType, ok := pqArrMapping[goType]
		goType, zero = "pq.GenericArray", "pg.GenericArray{}" // is of type struct { A any }; can't be nil
		if ok {
			goType, zero = arrType, "nil"
		}
	}
	return goType, zero, nil
}

// PostgresGoType parse a type into a Go type based on the database type
// definition.
func PostgresGoType(d xo.Type, schema, itype string) (string, string, error) {
	// SETOF -> []T
	if strings.HasPrefix(d.Type, "SETOF ") {
		d.Type = d.Type[len("SETOF "):]
		goType, _, err := PostgresGoType(d, schema, itype)
		if err != nil {
			return "", "", err
		}
		return "[]" + goType, "nil", nil
	}
	// If it's an array, the underlying type shouldn't also be set as an array
	typNullable := d.Nullable && !d.IsArray
	// special type handling
	typ := d.Type
	switch {
	case typ == `"char"`:
		typ = "char"
	case strings.HasPrefix(typ, "information_schema."):
		switch strings.TrimPrefix(typ, "information_schema.") {
		case "cardinal_number":
			typ = "integer"
		case "character_data", "sql_identifier", "yes_or_no":
			typ = "character varying"
		case "time_stamp":
			typ = "timestamp with time zone"
		}
	}
	var goType, zero string
	switch typ {
	case "boolean":
		goType, zero = "bool", "false"
		if typNullable {
			goType, zero = "sql.NullBool", "sql.NullBool{}"
		}
	case "bpchar", "character varying", "character", "inet", "money", "text", "name":
		goType, zero = "string", `""`
		if typNullable {
			goType, zero = "sql.NullString", "sql.NullString{}"
		}
	case "smallint":
		goType, zero = "int16", "0"
		if typNullable {
			goType, zero = "sql.NullInt64", "sql.NullInt64{}"
		}
	case "integer":
		goType, zero = itype, "0"
		if typNullable {
			goType, zero = "sql.NullInt64", "sql.NullInt64{}"
		}
	case "bigint":
		goType, zero = "int64", "0"
		if typNullable {
			goType, zero = "sql.NullInt64", "sql.NullInt64{}"
		}
	case "real":
		goType, zero = "float32", "0.0"
		if typNullable {
			goType, zero = "sql.NullFloat64", "sql.NullFloat64{}"
		}
	case "double precision", "numeric":
		goType, zero = "float64", "0.0"
		if typNullable {
			goType, zero = "sql.NullFloat64", "sql.NullFloat64{}"
		}
	case "date", "timestamp with time zone", "time with time zone", "time without time zone", "timestamp without time zone":
		goType, zero = "time.Time", "time.Time{}"
		if typNullable {
			goType, zero = "sql.NullTime", "sql.NullTime{}"
		}
	case "bit":
		goType, zero = "uint8", "0"
		if typNullable {
			goType, zero = "*uint8", "nil"
		}
	case "any", "bit varying", "bytea", "interval", "json", "jsonb", "xml":
		// TODO: write custom type for interval marshaling
		// TODO: marshalling for json types
		goType, zero = "[]byte", "nil"
	case "hstore":
		goType, zero = "hstore.Hstore", "nil"
	case "uuid":
		goType, zero = "uuid.UUID", "uuid.UUID{}"
		if typNullable {
			goType, zero = "uuid.NullUUID", "uuid.NullUUID{}"
		}
	default:
		goType, zero = schemaType(d.Type, typNullable, schema)
	}
	return goType, zero, nil
}

// PostgresTableColumns returns the columns for a table.
func PostgresTableColumns(ctx context.Context, db models.DB, schema string, table string) ([]*models.Column, error) {
	return models.PostgresTableColumns(ctx, db, schema, table, enableOids(ctx))
}

// PostgresIndexColumns returns the column list for an index.
//
// FIXME: rewrite this using SQL exclusively using OVER
func PostgresIndexColumns(ctx context.Context, db models.DB, schema string, table string, index string) ([]*models.IndexColumn, error) {
	// load columns
	cols, err := models.PostgresIndexColumns(ctx, db, schema, index)
	if err != nil {
		return nil, err
	}
	// load col order
	colOrd, err := models.PostgresGetColOrder(ctx, db, schema, index)
	if err != nil {
		return nil, err
	}
	// build schema name used in errors
	s := schema
	if s != "" {
		s += "."
	}
	// put cols in order using colOrder
	var ret []*models.IndexColumn
	for _, v := range strings.Split(colOrd.Ord, " ") {
		cid, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("could not convert %s%s index %s column %s to int", s, table, index, v)
		}
		// find column
		found := false
		var c *models.IndexColumn
		for _, ic := range cols {
			if cid == ic.Cid {
				found, c = true, ic
				break
			}
		}
		// sanity check
		if !found {
			return nil, fmt.Errorf("could not find %s%s index %s column id %d", s, table, index, cid)
		}
		ret = append(ret, c)
	}
	return ret, nil
}

// PostgresViewStrip strips '::type AS name' in queries.
func PostgresViewStrip(query, inspect []string) ([]string, []string, []string, error) {
	comments := make([]string, len(query))
	for i, line := range query {
		if pos := stripRE.FindStringIndex(line); pos != nil {
			query[i] = line[:pos[0]] + line[pos[1]:]
			comments[i] = line[pos[0]:pos[1]]
		}
	}
	return query, inspect, comments, nil
}

// stripRE is the regexp to match the '::type AS name' portion in a query,
// which is a quirk/requirement of generating queries for postgres.
var stripRE = regexp.MustCompile(`(?i)::[a-z][a-z0-9_\.]+\s+AS\s+[a-z][a-z0-9_\.]+`)

var pgStdArrMapping = map[string]string{
	"bool":    "[]bool",
	"[]byte":  "[][]byte",
	"float64": "[]float64",
	"float32": "[]float32",
	"int64":   "[]int64",
	"int32":   "[]int32",
	"string":  "[]string",
	// default: "[]byte"
}

var pqArrMapping = map[string]string{
	"bool":    "pq.BoolArray",
	"[]byte":  "pq.ByteArray",
	"float64": "pq.Float64Array",
	"float32": "pq.Float32Array",
	"int64":   "pq.Int64Array",
	"int32":   "pq.Int32Array",
	"string":  "pq.StringArray",
	// default: "pq.GenericArray"
}

// oidsKey is the oids context key.
const oidsKey xo.ContextKey = "oids"

// enableOids returns the enableOids value from the context.
func enableOids(ctx context.Context) bool {
	b, _ := ctx.Value(oidsKey).(bool)
	return b
}

// Composite type cache to avoid repeated database queries
var (
	compositeTypeCache      = make(map[string]map[string]bool)
	compositeTypeCacheMutex sync.RWMutex
)

// isCompositeType checks if the given type is a composite type in the specified schema.
// It uses a cache to avoid repeated database queries during type resolution.
func isCompositeType(ctx context.Context, typeName, schema string) bool {
	// Handle schema-prefixed types
	if strings.Contains(typeName, ".") {
		parts := strings.SplitN(typeName, ".", 2)
		if len(parts) == 2 {
			schema, typeName = parts[0], parts[1]
		}
	}

	// Check cache first
	compositeTypeCacheMutex.RLock()
	if schemaCache, ok := compositeTypeCache[schema]; ok {
		if isComposite, exists := schemaCache[typeName]; exists {
			compositeTypeCacheMutex.RUnlock()
			return isComposite
		}
	}
	compositeTypeCacheMutex.RUnlock()

	// Cache miss - need to load composite types for this schema
	return loadCompositeTypeCache(ctx, schema, typeName)
}

// loadCompositeTypeCache loads all composite types for a schema into the cache
// and returns whether the specific typeName is a composite type.
func loadCompositeTypeCache(ctx context.Context, schema, typeName string) bool {
	// Get database connection from context
	db, ok := ctx.Value(xo.DbKey).(*sql.DB)
	if !ok {
		return false // No database connection available
	}

	// Load all composite types for this schema
	compositeTypes, err := models.PostgresCompositeTypes(ctx, db, schema)
	if err != nil {
		return false // Error loading composite types
	}

	// Update cache
	compositeTypeCacheMutex.Lock()
	defer compositeTypeCacheMutex.Unlock()

	if compositeTypeCache[schema] == nil {
		compositeTypeCache[schema] = make(map[string]bool)
	}

	// Mark all found composite types
	isComposite := false
	for _, ct := range compositeTypes {
		compositeTypeCache[schema][ct.TypeName] = true
		if ct.TypeName == typeName {
			isComposite = true
		}
	}

	// If we didn't find the specific type, cache that it's NOT a composite type
	if !isComposite {
		compositeTypeCache[schema][typeName] = false
	}

	return isComposite
}

// compositeGoType handles Go type mapping for composite types.
func compositeGoType(typeName string, nullable bool, schema string) (string, string) {
	// Remove schema prefix if it matches the current schema
	if strings.HasPrefix(typeName, schema+".") {
		typeName = typeName[len(schema)+1:]
	}

	// Convert to Go type name (CamelCase)
	goType := snaker.SnakeToCamelIdentifier(typeName)
	zero := goType + "{}"

	// Handle nullable composite types with pointer
	if nullable {
		return "*" + goType, "nil"
	}

	return goType, zero
}

// PostgresGoTypeWithContext is a context-aware version of PostgresGoType
// that can detect composite types. This is used internally when context is available.
func PostgresGoTypeWithContext(ctx context.Context, d xo.Type, schema, itype string) (string, string, error) {
	// SETOF -> []T
	if strings.HasPrefix(d.Type, "SETOF ") {
		d.Type = d.Type[len("SETOF "):]
		goType, _, err := PostgresGoTypeWithContext(ctx, d, schema, itype)
		if err != nil {
			return "", "", err
		}
		return "[]" + goType, "nil", nil
	}

	// If it's an array, the underlying type shouldn't also be set as an array
	typNullable := d.Nullable && !d.IsArray

	// special type handling
	typ := d.Type
	switch {
	case typ == `"char"`:
		typ = "char"
	case strings.HasPrefix(typ, "information_schema."):
		switch strings.TrimPrefix(typ, "information_schema.") {
		case "cardinal_number":
			typ = "integer"
		case "character_data", "sql_identifier", "yes_or_no":
			typ = "character varying"
		case "time_stamp":
			typ = "timestamp with time zone"
		}
	}

	var goType, zero string
	switch typ {
	case "boolean":
		goType, zero = "bool", "false"
		if typNullable {
			goType, zero = "sql.NullBool", "sql.NullBool{}"
		}
	case "bpchar", "character varying", "character", "inet", "money", "text", "name":
		goType, zero = "string", `""`
		if typNullable {
			goType, zero = "sql.NullString", "sql.NullString{}"
		}
	case "smallint":
		goType, zero = "int16", "0"
		if typNullable {
			goType, zero = "sql.NullInt64", "sql.NullInt64{}"
		}
	case "integer":
		goType, zero = itype, "0"
		if typNullable {
			goType, zero = "sql.NullInt64", "sql.NullInt64{}"
		}
	case "bigint":
		goType, zero = "int64", "0"
		if typNullable {
			goType, zero = "sql.NullInt64", "sql.NullInt64{}"
		}
	case "real":
		goType, zero = "float32", "0.0"
		if typNullable {
			goType, zero = "sql.NullFloat64", "sql.NullFloat64{}"
		}
	case "double precision", "numeric":
		goType, zero = "float64", "0.0"
		if typNullable {
			goType, zero = "sql.NullFloat64", "sql.NullFloat64{}"
		}
	case "date", "timestamp with time zone", "time with time zone", "time without time zone", "timestamp without time zone":
		goType, zero = "time.Time", "time.Time{}"
		if typNullable {
			goType, zero = "sql.NullTime", "sql.NullTime{}"
		}
	case "bit":
		goType, zero = "uint8", "0"
		if typNullable {
			goType, zero = "*uint8", "nil"
		}
	case "any", "bit varying", "bytea", "interval", "json", "jsonb", "xml":
		goType, zero = "[]byte", "nil"
	case "hstore":
		goType, zero = "hstore.Hstore", "nil"
	case "uuid":
		goType, zero = "uuid.UUID", "uuid.UUID{}"
		if typNullable {
			goType, zero = "uuid.NullUUID", "uuid.NullUUID{}"
		}
	default:
		// Check if it's a composite type before falling back to schemaType
		if isCompositeType(ctx, d.Type, schema) {
			goType, zero = compositeGoType(d.Type, typNullable, schema)
		} else {
			goType, zero = schemaType(d.Type, typNullable, schema)
		}
	}
	return goType, zero, nil
}
