package tests

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

// Test database setup and teardown
func setupTestDB(t *testing.T) *sql.DB {
	// Get database URL from environment or use default
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost/testdb?sslmode=disable"
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL not reachable: %v", err)
	}

	return db
}

func createTestSchema(t *testing.T, db *sql.DB) {
	schema := `
		-- Drop existing types if they exist (reverse order due to dependencies)
		DROP TABLE IF EXISTS test_customers CASCADE;
		DROP TYPE IF EXISTS test_person CASCADE;
		DROP TYPE IF EXISTS test_address CASCADE;
		DROP TYPE IF EXISTS test_company CASCADE;

		-- Create simple composite type
		CREATE TYPE test_address AS (
			street TEXT,
			city TEXT,
			zipcode INTEGER
		);

		-- Create nested composite type
		CREATE TYPE test_person AS (
			name TEXT,
			age INTEGER,
			home_address test_address
		);

		-- Create composite type with arrays
		CREATE TYPE test_company AS (
			name TEXT,
			addresses test_address[],
			founded TIMESTAMP
		);

		-- Create test table
		CREATE TABLE test_customers (
			id SERIAL PRIMARY KEY,
			profile test_person,
			billing_address test_address,
			company_info test_company,
			notes TEXT
		);
	`

	_, err := db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create test schema: %v", err)
	}
}

func TestPostgreSQLCompositeTypeRoundTrip(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	tests := []struct {
		name            string
		profile         string
		billingAddress  string
		expectedProfile string
		expectedBilling string
	}{
		{
			name:            "simple composite",
			profile:         `("John Doe",30,"(""123 Main St"",""Anytown"",12345)")`,
			billingAddress:  `("456 Oak Ave","Somewhere",67890)`,
			expectedProfile: `("John Doe",30,"(""123 Main St"",""Anytown"",12345)")`,
			expectedBilling: `("456 Oak Ave","Somewhere",67890)`,
		},
		{
			name:            "composite with quotes and commas",
			profile:         `("Jane ""Jo"" Smith",25,"(""456 Oak Ave, Apt 2B"",""Some, City"",67890)")`,
			billingAddress:  `("789 Pine St, Unit 3C","Another, City",11111)`,
			expectedProfile: `("Jane ""Jo"" Smith",25,"(""456 Oak Ave, Apt 2B"",""Some, City"",67890)")`,
			expectedBilling: `("789 Pine St, Unit 3C","Another, City",11111)`,
		},
		{
			name:            "composite with NULL fields",
			profile:         `("Bob Smith",45,"(""999 Elm St"",NULL,22222)")`,
			billingAddress:  `(NULL,"Valid City",33333)`,
			expectedProfile: `("Bob Smith",45,"(""999 Elm St"",,22222)")`,
			expectedBilling: `(,"Valid City",33333)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Insert test data
			insertSQL := `
				INSERT INTO test_customers (profile, billing_address, notes) 
				VALUES ($1, $2, $3) 
				RETURNING id, profile, billing_address
			`

			var id int
			var retrievedProfile, retrievedBilling string

			err := db.QueryRow(insertSQL, tt.profile, tt.billingAddress, "test").
				Scan(&id, &retrievedProfile, &retrievedBilling)

			if err != nil {
				t.Fatalf("Failed to insert/retrieve data: %v", err)
			}

			t.Logf("Inserted ID: %d", id)
			t.Logf("Expected profile: %s", tt.expectedProfile)
			t.Logf("Retrieved profile: %s", retrievedProfile)
			t.Logf("Expected billing: %s", tt.expectedBilling)
			t.Logf("Retrieved billing: %s", retrievedBilling)

			// Clean up
			_, err = db.Exec("DELETE FROM test_customers WHERE id = $1", id)
			if err != nil {
				t.Errorf("Failed to clean up test data: %v", err)
			}
		})
	}
}

func TestPostgreSQLArrayOfComposites(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	// Test array of composite types
	companySQL := `
		INSERT INTO test_customers (company_info, notes) 
		VALUES ($1, $2) 
		RETURNING id, company_info
	`

	// Array of addresses
	arrayOfAddresses := `("Acme Corp","{""(\\""123 Main St\\"",\\""City1\\"",12345)"",""(\\""456 Oak Ave\\"",\\""City2\\"",67890)""}","2020-01-01 00:00:00")`

	var id int
	var retrievedCompany string

	err := db.QueryRow(companySQL, arrayOfAddresses, "test company").
		Scan(&id, &retrievedCompany)

	if err != nil {
		t.Fatalf("Failed to insert/retrieve company data: %v", err)
	}

	t.Logf("Company ID: %d", id)
	t.Logf("Retrieved company: %s", retrievedCompany)

	// Clean up
	_, err = db.Exec("DELETE FROM test_customers WHERE id = $1", id)
	if err != nil {
		t.Errorf("Failed to clean up test data: %v", err)
	}
}

func TestPostgreSQLNullComposites(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	// Test NULL composite values
	insertSQL := `
		INSERT INTO test_customers (profile, billing_address, notes) 
		VALUES ($1, $2, $3) 
		RETURNING id, profile, billing_address
	`

	tests := []struct {
		name           string
		profile        interface{}
		billingAddress interface{}
	}{
		{"null profile", nil, `("456 Oak Ave","Somewhere",67890)`},
		{"null billing", `("John Doe",30,"(""123 Main St"",""Anytown"",12345)")`, nil},
		{"both null", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var id int
			var retrievedProfile, retrievedBilling sql.NullString

			err := db.QueryRow(insertSQL, tt.profile, tt.billingAddress, "test null").
				Scan(&id, &retrievedProfile, &retrievedBilling)

			if err != nil {
				t.Fatalf("Failed to insert/retrieve NULL data: %v", err)
			}

			t.Logf("ID: %d", id)
			t.Logf("Profile valid: %v, value: %s", retrievedProfile.Valid, retrievedProfile.String)
			t.Logf("Billing valid: %v, value: %s", retrievedBilling.Valid, retrievedBilling.String)

			// Clean up
			_, err = db.Exec("DELETE FROM test_customers WHERE id = $1", id)
			if err != nil {
				t.Errorf("Failed to clean up test data: %v", err)
			}
		})
	}
}

func TestPostgreSQLCompositeTypeQueries(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	// Insert test data
	testData := []struct {
		profile        string
		billingAddress string
		notes          string
	}{
		{
			profile:        `("Alice Johnson",28,"(""111 First St"",""Boston"",02101)")`,
			billingAddress: `("111 First St","Boston",02101)`,
			notes:          "Customer 1",
		},
		{
			profile:        `("Bob Wilson",35,"(""222 Second St"",""New York"",10001)")`,
			billingAddress: `("333 Third St","Brooklyn",11201)`,
			notes:          "Customer 2",
		},
		{
			profile:        `("Carol Davis",42,"(""444 Fourth St"",""Boston"",02102)")`,
			billingAddress: `("555 Fifth St","Boston",02103)`,
			notes:          "Customer 3",
		},
	}

	// Insert all test data
	var ids []int
	for _, data := range testData {
		var id int
		err := db.QueryRow(
			"INSERT INTO test_customers (profile, billing_address, notes) VALUES ($1, $2, $3) RETURNING id",
			data.profile, data.billingAddress, data.notes,
		).Scan(&id)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
		ids = append(ids, id)
	}

	// Test querying by composite field access
	t.Run("query_by_composite_field", func(t *testing.T) {
		// Query customers in Boston using composite type field access
		rows, err := db.Query(`
			SELECT id, notes, (profile).name, (billing_address).city 
			FROM test_customers 
			WHERE (billing_address).city = 'Boston'
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to query by composite field: %v", err)
		}
		defer rows.Close()

		var results []struct {
			id     int
			notes  string
			name   string
			city   string
		}

		for rows.Next() {
			var result struct {
				id    int
				notes string
				name  string
				city  string
			}
			err := rows.Scan(&result.id, &result.notes, &result.name, &result.city)
			if err != nil {
				t.Fatalf("Failed to scan result: %v", err)
			}
			results = append(results, result)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 Boston customers, got %d", len(results))
		}

		for _, result := range results {
			if result.city != "Boston" {
				t.Errorf("Expected city 'Boston', got '%s'", result.city)
			}
			t.Logf("Boston customer: ID=%d, Name=%s, City=%s, Notes=%s",
				result.id, result.name, result.city, result.notes)
		}
	})

	// Test updating composite fields
	t.Run("update_composite_field", func(t *testing.T) {
		newAddress := `("999 Updated St","Updated City",99999)`

		result, err := db.Exec(
			"UPDATE test_customers SET billing_address = $1 WHERE id = $2",
			newAddress, ids[0],
		)
		if err != nil {
			t.Fatalf("Failed to update composite field: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", rowsAffected)
		}

		// Verify the update
		var updatedAddress string
		err = db.QueryRow(
			"SELECT billing_address FROM test_customers WHERE id = $1",
			ids[0],
		).Scan(&updatedAddress)
		if err != nil {
			t.Fatalf("Failed to verify update: %v", err)
		}

		t.Logf("Updated address: %s", updatedAddress)
	})

	// Clean up all test data
	for _, id := range ids {
		_, err := db.Exec("DELETE FROM test_customers WHERE id = $1", id)
		if err != nil {
			t.Errorf("Failed to clean up test data for ID %d: %v", id, err)
		}
	}
}

func TestPostgreSQLCompositeTypeMetadata(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	// Test the queries that dbtpl uses to discover composite types
	t.Run("discover_composite_types", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT t.typname, n.nspname, COALESCE(obj_description(t.oid, 'pg_type'), '')
			FROM pg_type t 
			JOIN ONLY pg_namespace n ON n.oid = t.typnamespace 
			WHERE t.typtype = 'c' 
			AND t.typrelid > 0 
			AND n.nspname = 'public'
			AND t.typname LIKE 'test_%'
			ORDER BY t.typname
		`)
		if err != nil {
			t.Fatalf("Failed to discover composite types: %v", err)
		}
		defer rows.Close()

		var types []struct {
			typeName   string
			schemaName string
			comment    string
		}

		for rows.Next() {
			var typ struct {
				typeName   string
				schemaName string
				comment    string
			}
			err := rows.Scan(&typ.typeName, &typ.schemaName, &typ.comment)
			if err != nil {
				t.Fatalf("Failed to scan composite type: %v", err)
			}
			types = append(types, typ)
		}

		expectedTypes := []string{"test_address", "test_company", "test_person"}
		if len(types) != len(expectedTypes) {
			t.Errorf("Expected %d composite types, got %d", len(expectedTypes), len(types))
		}

		for i, typ := range types {
			if i < len(expectedTypes) && typ.typeName != expectedTypes[i] {
				t.Errorf("Expected type %s, got %s", expectedTypes[i], typ.typeName)
			}
			t.Logf("Discovered composite type: %s.%s (%s)", typ.schemaName, typ.typeName, typ.comment)
		}
	})

	// Test discovering composite type attributes
	t.Run("discover_composite_attributes", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT a.attnum, a.attname, format_type(a.atttypid, a.atttypmod), 
			       a.attnotnull, COALESCE(d.description, '')
			FROM pg_attribute a 
			JOIN pg_type t ON t.typrelid = a.attrelid 
			JOIN ONLY pg_namespace n ON n.oid = t.typnamespace 
			LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum 
			WHERE t.typtype = 'c' 
			AND t.typrelid > 0 
			AND n.nspname = 'public'
			AND t.typname = 'test_address'
			AND a.attisdropped = false 
			AND a.attnum > 0 
			ORDER BY a.attnum
		`)
		if err != nil {
			t.Fatalf("Failed to discover composite attributes: %v", err)
		}
		defer rows.Close()

		var attrs []struct {
			fieldOrdinal int
			attrName     string
			dataType     string
			notNull      bool
			comment      string
		}

		for rows.Next() {
			var attr struct {
				fieldOrdinal int
				attrName     string
				dataType     string
				notNull      bool
				comment      string
			}
			err := rows.Scan(&attr.fieldOrdinal, &attr.attrName, &attr.dataType, &attr.notNull, &attr.comment)
			if err != nil {
				t.Fatalf("Failed to scan attribute: %v", err)
			}
			attrs = append(attrs, attr)
		}

		expectedAttrs := []struct {
			name     string
			dataType string
		}{
			{"street", "text"},
			{"city", "text"},
			{"zipcode", "integer"},
		}

		if len(attrs) != len(expectedAttrs) {
			t.Errorf("Expected %d attributes, got %d", len(expectedAttrs), len(attrs))
		}

		for i, attr := range attrs {
			if i < len(expectedAttrs) {
				if attr.attrName != expectedAttrs[i].name {
					t.Errorf("Expected attribute %s, got %s", expectedAttrs[i].name, attr.attrName)
				}
				if attr.dataType != expectedAttrs[i].dataType {
					t.Errorf("Expected data type %s, got %s", expectedAttrs[i].dataType, attr.dataType)
				}
			}
			t.Logf("Attribute %d: %s %s (not_null: %v)", attr.fieldOrdinal, attr.attrName, attr.dataType, attr.notNull)
		}
	})
}

// Performance test to ensure composite types don't have significant overhead
func BenchmarkPostgreSQLCompositeTypeOperations(b *testing.B) {
	db := setupTestDB(&testing.T{})
	defer db.Close()

	createTestSchema(&testing.T{}, db)

	// Prepare test data
	profile := `("John Doe",30,"(""123 Main St"",""Anytown"",12345)")`
	billing := `("456 Oak Ave","Somewhere",67890)`

	b.Run("insert_composite", func(b *testing.B) {
		stmt, err := db.Prepare("INSERT INTO test_customers (profile, billing_address, notes) VALUES ($1, $2, $3)")
		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := stmt.Exec(profile, billing, fmt.Sprintf("benchmark %d", i))
			if err != nil {
				b.Fatalf("Failed to insert: %v", err)
			}
		}
	})

	// Clean up benchmark data
	_, err := db.Exec("DELETE FROM test_customers WHERE notes LIKE 'benchmark %'")
	if err != nil {
		b.Errorf("Failed to clean up benchmark data: %v", err)
	}
}

func TestPostgreSQLEdgeCases(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedResult string
	}{
		{
			name:           "empty_string_fields",
			input:          `("","",0)`,
			expectError:    false,
			expectedResult: `("","",0)`,
		},
		{
			name:           "single_quotes_in_field",
			input:          `("O'Reilly Street","O'Fallon",12345)`,
			expectError:    false,
			expectedResult: `("O'Reilly Street","O'Fallon",12345)`,
		},
		{
			name:           "backslashes_in_field",
			input:          `("C:\\Windows\\System32","Windows",0)`,
			expectError:    false,
			expectedResult: `("C:\\Windows\\System32","Windows",0)`,
		},
		{
			name:           "unicode_characters",
			input:          `("Café München","Zürich",8001)`,
			expectError:    false,
			expectedResult: `("Café München","Zürich",8001)`,
		},
		{
			name:           "very_large_numbers",
			input:          `("Test Street","Test City",2147483647)`,
			expectError:    false,
			expectedResult: `("Test Street","Test City",2147483647)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var id int
			var result string

			err := db.QueryRow(
				"INSERT INTO test_customers (billing_address, notes) VALUES ($1, $2) RETURNING id, billing_address",
				tt.input, "edge case test",
			).Scan(&id, &result)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.expectError {
				t.Logf("Input: %s", tt.input)
				t.Logf("Result: %s", result)

				// Clean up
				_, err = db.Exec("DELETE FROM test_customers WHERE id = $1", id)
				if err != nil {
					t.Errorf("Failed to clean up: %v", err)
				}
			}
		})
	}
}

// Test concurrent access to composite types
func TestPostgreSQLConcurrentAccess(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	createTestSchema(t, db)

	const numGoroutines = 10
	const numOperations = 100

	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < numOperations; j++ {
				profile := fmt.Sprintf(`("User %d-%d",25,"(""Street %d-%d"",""City %d-%d"",%d)")`,
					goroutineID, j, goroutineID, j, goroutineID, j, 10000+goroutineID*1000+j)

				var id int
				err := db.QueryRow(
					"INSERT INTO test_customers (profile, notes) VALUES ($1, $2) RETURNING id",
					profile, fmt.Sprintf("concurrent test %d-%d", goroutineID, j),
				).Scan(&id)

				if err != nil {
					errChan <- fmt.Errorf("goroutine %d operation %d: %w", goroutineID, j, err)
					return
				}

				// Immediately delete to keep table small
				_, err = db.Exec("DELETE FROM test_customers WHERE id = $1", id)
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d cleanup %d: %w", goroutineID, j, err)
					return
				}
			}
			errChan <- nil
		}(i)
	}

	// Collect results
	var errors []error
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		t.Errorf("Concurrent test had %d errors:", len(errors))
		for _, err := range errors {
			t.Errorf("  %v", err)
		}
	}

	t.Logf("Concurrent test completed: %d goroutines × %d operations = %d total operations",
		numGoroutines, numOperations, numGoroutines*numOperations)
}