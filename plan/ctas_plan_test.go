package plan_test

import (
	"embed"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/literal"
	"github.com/substrait-io/substrait-go/plan"
	substraitproto "github.com/substrait-io/substrait-go/proto"
	"github.com/substrait-io/substrait-go/types"
	"testing"
)

type ctasTestData struct {
	name             string
	projectionInScan []int
	columnsInProject []int
	functionRefs     []extensions.ID
	ctasTableName    string
	ctasTableSchema  types.NamedStruct
	useFilter        bool
}

//go:embed testdata/*.json
var testdata embed.FS

var employeeSchema = types.NamedStruct{Names: []string{"employee_id", "name", "department_id", "salary", "role"},
	Struct: types.StructType{
		Nullability: types.NullabilityRequired,
		Types: []types.Type{
			&types.Int32Type{Nullability: types.NullabilityRequired},
			&types.StringType{Nullability: types.NullabilityNullable},
			&types.Int32Type{Nullability: types.NullabilityNullable},
			&types.DecimalType{Precision: 10, Scale: 2, Nullability: types.NullabilityNullable},
			&types.StringType{Nullability: types.NullabilityNullable},
		},
	}}

var employeeSalariesSchema = types.NamedStruct{Names: []string{"name", "salary"},
	Struct: types.StructType{
		Types: []types.Type{
			&types.StringType{Nullability: types.NullabilityNullable},
			&types.DecimalType{Precision: 10, Scale: 2, Nullability: types.NullabilityNullable},
		},
	}}

var employeeSchemaNullable = types.NamedStruct{Names: []string{"employee_id", "name", "department_id", "salary", "role"},
	Struct: types.StructType{
		Types: []types.Type{
			&types.Int32Type{Nullability: types.NullabilityNullable},
			&types.StringType{Nullability: types.NullabilityNullable},
			&types.Int32Type{Nullability: types.NullabilityNullable},
			&types.DecimalType{Precision: 10, Scale: 2, Nullability: types.NullabilityNullable},
			&types.StringType{Nullability: types.NullabilityNullable},
		},
	}}

// LoadJSONFileAsString reads the entire JSON file content as a string.
func loadJSONFileAsString(filename string) (string, error) {
	jsonData, err := testdata.ReadFile(fmt.Sprintf("testdata/%s", filename))
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func initPlanBuilder(functionRefs []extensions.ID) plan.Builder {
	b := plan.NewBuilderDefault()
	for _, f := range functionRefs {
		b.GetFunctionRef(f.URI, f.Name)
	}
	return b
}

func makeScanRel(b plan.Builder, projection []int) plan.Rel {
	structItems := make([]*substraitproto.Expression_MaskExpression_StructItem, len(projection))
	for i, p := range projection {
		structItems[i] = &substraitproto.Expression_MaskExpression_StructItem{Field: int32(p)}
	}

	projExpr := expr.MaskExpressionFromProto(
		&substraitproto.Expression_MaskExpression{
			Select: &substraitproto.Expression_MaskExpression_StructSelect{
				StructItems: structItems,
			},
			MaintainSingularStruct: true,
		})

	scan := b.NamedScan([]string{"employees"}, employeeSchema)
	scan.SetProjection(projExpr)
	return scan
}

// This makes the filter with condition "role LIKE '%Engineer%'"
func makeFilterRel(t *testing.T, b plan.Builder, scan plan.Rel, td ctasTestData) plan.Rel {
	role_id_col, err := b.RootFieldRef(scan, int32(0))
	require.NoError(t, err)
	engineerLiteral, err := literal.NewString("Engineer")
	require.NoError(t, err)
	scalarExpr, err := b.ScalarFn(td.functionRefs[0].URI, td.functionRefs[0].Name, nil, role_id_col, engineerLiteral)
	require.NoError(t, err)
	filterRel, err := b.Filter(scan, scalarExpr)
	require.NoError(t, err)
	return filterRel
}

func makeProjectRel(t *testing.T, b plan.Builder, td ctasTestData) plan.Rel {
	var projectInput plan.Rel
	if td.useFilter {
		scan := makeScanRel(b, td.projectionInScan)
		projectInput = makeFilterRel(t, b, scan, td)
	} else {
		projectInput = makeScanRel(b, td.projectionInScan)
	}

	refs := make([]expr.Expression, len(td.columnsInProject))
	for i, c := range td.columnsInProject {
		ref, err := b.RootFieldRef(projectInput, int32(c))
		require.NoError(t, err)
		refs[i] = ref
	}
	project, err := b.Project(projectInput, refs...)
	require.NoError(t, err)
	return project
}

func TestCreateTableAsSelectRoundTrip(t *testing.T) {
	for _, td := range []ctasTestData{
		{
			"ctas_basic",
			[]int{1, 3},
			[]int{0, 1},
			[]extensions.ID{},
			"employee_salaries",
			employeeSalariesSchema,
			false,
		},
		{
			"ctas_with_filter",
			[]int{4, 0, 1, 2, 3},
			[]int{1, 2, 3, 4, 0},
			[]extensions.ID{
				{
					"https://github.com/substrait-io/substrait/blob/main/extensions/functions_string.yaml",
					"contains:str_str",
				},
			},
			"filtered_employees",
			employeeSchemaNullable,
			true,
		},
	} {
		t.Run(td.name, func(t *testing.T) {
			// Load the expected JSON. This will be our baseline for comparison.
			expectedJson, err := loadJSONFileAsString(fmt.Sprintf("%s.json", td.name))
			require.NoError(t, err)

			// generate the plan using builder
			b := initPlanBuilder(td.functionRefs)
			project := makeProjectRel(t, b, td)
			create, err := b.CreateTableAsSelect(project, []string{"main", td.ctasTableName}, td.ctasTableSchema)
			require.NoError(t, err)
			p, err := b.Plan(create, td.ctasTableSchema.Names)
			require.NoError(t, err)

			// Check that the generated plan matches the expected JSON.
			checkRoundTrip(t, expectedJson, p)
		})
	}
}
