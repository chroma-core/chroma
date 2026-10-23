# Operator Constants

## Adding a New Task Operator

1. Create a database migration (in `go/pkg/sysdb/metastore/db/migrations/*.sql`)

2. Add constants to `go/pkg/sysdb/metastore/db/dbmodel/constants.go`:
   ```go
   OperatorMyOperator = uuid.MustParse("your-uuid-here")
   OperatorNameMyOperator = "my_operator"
   ```

3. Generate Rust constants:
   ```bash
   ./bin/generate_operator_constants.sh
   ```

4. Commit:
   - `go/pkg/sysdb/metastore/db/dbmodel/constants.go`
   - `rust/types/src/operators_generated.rs`

The test `test_k8s_integration_operator_constants` will verify everything is in sync.
