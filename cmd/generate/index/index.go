package index

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/spf13/cobra"

	"github.com/openfga/openfga/pkg/typesystem"
)

type NamedSQLStatement struct {
	name string
	sql  string
}

type materializations struct {
	views      []string
	statements map[string]NamedSQLStatement
}

func NewGenerateIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "FGA Index materialization generator",
		Run:   generateIndex,
		Args:  cobra.NoArgs,
	}

	flags := cmd.Flags()

	flags.String("name", "", "a unique name for the index")

	flags.String("file", "model.fga", "an absolute file path to an FGA model (default 'model.fga')")

	flags.String("output", "", "an absolute file path to the output file")

	flags.String("object-type", "", "the object type to prodouce an index materialization for (e.g. document)")

	flags.String("relations", "", "the relation to produce an index materialization for (e.g. viewer)")

	flags.String("subject-type", "", "the type of the user/subject to produce an index materialization for (e.g. group)")
	flags.String("subject-relation", "", "the type of the user/subject to produce an index materialization for (e.g. member)")

	return cmd
}

func generateIndex(cmd *cobra.Command, args []string) {
	indexName, err := cmd.Flags().GetString("name")
	if err != nil {
		panic("'name' is a required flag")
	}

	modelFile, err := cmd.Flags().GetString("file")
	if err != nil {
		panic("'file' is a required flag")
	}

	outputFilePath, err := cmd.Flags().GetString("output")
	if err != nil {
		panic("'file' is a required flag")
	}

	objectType, err := cmd.Flags().GetString("object-type")
	if err != nil {
		//panic("'object-type' is a required flag")
	}

	relations, err := cmd.Flags().GetStringSlice("relations")
	if err != nil {
		//panic("'relations' is a required flag")
	}

	subjectType, err := cmd.Flags().GetString("subject-type")
	if err != nil {
		//panic("'subject-type' is a required flag")
	}

	subjectRelation, err := cmd.Flags().GetString("subject-relation")
	if err != nil {
		//panic("'subject-type' is a required flag")
	}

	modelBytes, err := os.ReadFile(modelFile)
	if err != nil {
		panic(fmt.Sprintf("model file error: %v", err))
	}

	model := parser.MustTransformDSLToProto(string(modelBytes))

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	if err != nil {
		panic(err)
	}

	sql := materialize(materializationInput{
		indexName,
		typesys,
		objectType,
		relations,
		subjectType,
		subjectRelation,
	})

	if outputFilePath != "" {
		outputFile, err := os.Create(outputFilePath)
		if err != nil {
			panic(fmt.Sprintf("failed to open output file '%s': %v", outputFilePath, err))
		}

		_, err = outputFile.WriteString(sql)
		if err != nil {
			panic(fmt.Sprintf("failed to write to output file: %v", err))
		}
	} else {
		fmt.Println(sql)
	}
}

type materializationInput struct {
	indexName       string
	typesys         *typesystem.TypeSystem
	objectType      string
	relations       []string
	subjectType     string
	subjectRelation string
}

// materialize produces one or more statements defining the materialized views that
// materialize some FGA index.
func materialize(in materializationInput) string {
	typesys := in.typesys

	statements := map[string]NamedSQLStatement{}
	for objectType, relations := range typesys.GetAllRelations() {
		for relationName, _ := range relations {
			namedStatement := materializeInternal(typesys, objectType, relationName)

			statements[namedStatement.name] = namedStatement
		}
	}

	var viewbody string
	var viewselect string

	indexEverything := in.objectType == "" && len(in.relations) == 0 && in.subjectType == "" && in.subjectRelation == ""

	i := 0
	for statementName, statement := range statements {
		if i >= len(statements)-1 {
			viewbody += fmt.Sprintf(`%s(subject_type TEXT, subject_id TEXT, subject_relation TEXT, relation TEXT, object_type TEXT, object_id TEXT) AS (%s)`, statementName, statement.sql)

			if indexEverything {
				viewselect += fmt.Sprintf("SELECT * FROM %s", statementName)
			}
		} else {
			viewbody += fmt.Sprintf(`%s(subject_type TEXT, subject_id TEXT, subject_relation TEXT, relation TEXT, object_type TEXT, object_id TEXT) AS (%s),`, statementName, statement.sql)

			if indexEverything {
				viewselect += fmt.Sprintf(`SELECT * FROM %s UNION ALL `, statementName)
			}
		}
		i += 1
	}

	if !indexEverything {
		var quotedRelations []string
		for _, relation := range in.relations {
			quotedRelations = append(quotedRelations, fmt.Sprintf(`'%s'`, relation))
		}

		viewselect = fmt.Sprintf(`SELECT * FROM %s WHERE relation IN (%s) AND subject_type='%s' AND subject_relation='%s'`, fmt.Sprintf("%s_%s", in.objectType, quotedRelations, in.subjectType, in.subjectRelation))
	}

	statement := fmt.Sprintf(`
	CREATE VIEW %s AS WITH MUTUALLY RECURSIVE
		%s

	%s;`, in.indexName, viewbody, viewselect)

	return statement
}

func materializeInternal(
	typesys *typesystem.TypeSystem,
	objectType, relation string,
) NamedSQLStatement {
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		panic(err)
	}

	return materializeInternalWithRewrite(typesys, objectType, relation, rel.GetRewrite())
}

func materializeInternalWithRewrite(
	typesys *typesystem.TypeSystem,
	objectType string,
	relation string,
	rewrite *openfgav1.Userset,
) NamedSQLStatement {
	switch rewrite := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return materializeDirect(typesys, objectType, relation)
	case *openfgav1.Userset_ComputedUserset:
		rewrittenRelation := rewrite.ComputedUserset.GetRelation()
		return materializeComputedUserset(objectType, relation, rewrittenRelation)
	case *openfgav1.Userset_TupleToUserset:
		return materializeTupleToUserset(typesys, objectType, relation, rewrite)
	case *openfgav1.Userset_Union:
		var sql string

		childRewrites := rewrite.Union.GetChild()
		for i, childRewrite := range childRewrites {
			s := materializeInternalWithRewrite(typesys, objectType, relation, childRewrite)
			sql += s.sql

			if i < len(childRewrites)-1 {
				sql += " UNION "
			}
		}

		return NamedSQLStatement{
			name: fmt.Sprintf("%s_%s", objectType, relation),
			sql:  sql,
		}

	case *openfgav1.Userset_Intersection:
		var sql string

		operands := []string{}

		childRewrites := rewrite.Intersection.GetChild()
		for i, childRewrite := range childRewrites {
			operandStatementName := fmt.Sprintf("operand_%d", i)
			operands = append(operands, operandStatementName)

			if i == 0 {
				sql += fmt.Sprintf("WITH %s AS (", operandStatementName)
			} else {
				sql += fmt.Sprintf("%s AS (", operandStatementName)
			}

			s := materializeInternalWithRewrite(typesys, objectType, relation, childRewrite)
			sql += s.sql

			if i < len(childRewrites)-1 {
				sql += "), "
			} else {
				sql += ")"
			}
		}

		if len(childRewrites) > 1 {
			sql += fmt.Sprintf("SELECT subject_type, subject_id, subject_relation, relation, object_type, object_id FROM %s WHERE EXISTS (SELECT FROM %s)", operands[0], strings.Join(operands[1:], ","))
		} else {
			sql += fmt.Sprintf("SELECT subject_type, subject_id, subject_relation, relation, object_type, object_id FROM %s", operands[0])
		}

		return NamedSQLStatement{
			name: fmt.Sprintf("%s_%s", objectType, relation),
			sql:  sql,
		}

	case *openfgav1.Userset_Difference:
		baseRewrite := rewrite.Difference.GetBase()
		subtractRewrite := rewrite.Difference.GetSubtract()

		baseStatement := materializeInternalWithRewrite(typesys, objectType, relation, baseRewrite)

		subtractStatement := materializeInternalWithRewrite(typesys, objectType, relation, subtractRewrite)

		sql := fmt.Sprintf(`WITH base AS (%s), subtract AS (%s) SELECT subject_type, subject_id, subject_relation, '%s', object_type, object_id FROM base b WHERE NOT EXISTS (SELECT FROM subtract s WHERE b.subject_type=s.subject_type AND b.subject_id=s.subject_id AND b.object_type=s.object_type AND b.object_id=s.object_id)`, baseStatement.sql, subtractStatement.sql, relation)

		return NamedSQLStatement{
			name: fmt.Sprintf("%s_%s", objectType, relation),
			sql:  sql,
		}
	default:
		panic("rewrite unsupported for indexing at this time")
	}
}

func materializeDirect(
	typesys *typesystem.TypeSystem,
	objectType string,
	relation string,
) NamedSQLStatement {
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		panic(err)
	}

	statement := NamedSQLStatement{
		name: fmt.Sprintf("%s_%s", objectType, relation),
	}

	var subjectTypes []string
	var nestedStatements []string
	for _, subjectRelationRef := range rel.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		subjectType := subjectRelationRef.GetType()

		if subjectRelationRef.GetRelationOrWildcard() == nil {
			subjectTypes = append(subjectTypes, fmt.Sprintf(`'%s'`, subjectType))
			continue
		}

		if subjectRelationRef.GetRelation() != "" {
			subjectRelation := subjectRelationRef.GetRelation()

			referencedTableName := fmt.Sprintf("%s_%s", subjectType, subjectRelation)

			sql := fmt.Sprintf(`SELECT r.subject_type, r.subject_id, r.subject_relation, '%s', s.object_type, s.object_id FROM %s r, tuples s WHERE s.subject_type = '%s' AND s.subject_relation = '%s' AND
			  s.relation = '%s' AND s.object_type = '%s' AND
			  s.subject_type = r.object_type AND s.subject_id = r.object_id AND
			  s.subject_relation = r.relation`, relation, referencedTableName, subjectType, subjectRelation, relation, objectType)

			nestedStatements = append(nestedStatements, sql)
		}
	}

	statement.sql = fmt.Sprintf(`SELECT subject_type, subject_id, subject_relation, relation, object_type,object_id FROM tuples WHERE object_type='%s' AND relation='%s' AND subject_type IN (%s) AND subject_relation=''`, objectType, relation, strings.Join(subjectTypes, ","))

	if len(nestedStatements) > 0 {
		statement.sql += " UNION "
	}

	for i, nestedStatement := range nestedStatements {
		if i < len(nestedStatements)-1 {
			statement.sql += fmt.Sprintf("%s UNION ", nestedStatement)
		} else {
			statement.sql += nestedStatement
		}
	}

	return statement
}

func materializeComputedUserset(
	objectType string,
	relation string,
	rewrittenRelation string,
) NamedSQLStatement {
	rewrittenStatementName := fmt.Sprintf("%s_%s", objectType, rewrittenRelation)

	return NamedSQLStatement{
		name: fmt.Sprintf("%s_%s", objectType, relation),
		sql:  fmt.Sprintf(`SELECT subject_type, subject_id, subject_relation, '%s', object_type,object_id FROM %s`, relation, rewrittenStatementName),
	}
}

func materializeTupleToUserset(
	typesys *typesystem.TypeSystem,
	objectType string,
	relation string,
	ttuRewrite *openfgav1.Userset_TupleToUserset,
) NamedSQLStatement {
	tuplesetRelation := ttuRewrite.TupleToUserset.GetTupleset().GetRelation()
	computedRelation := ttuRewrite.TupleToUserset.GetComputedUserset().GetRelation()

	relatedTypes, err := typesys.GetDirectlyRelatedUserTypes(objectType, tuplesetRelation)
	if err != nil {
		// todo: handle error
		panic(err)
	}

	var subjectTypes []string // parent: [folder, org] - subjectTypes are ('folder', 'org')
	var quotedSubjectTypes []string
	for _, relatedType := range relatedTypes {
		subjectType := relatedType.GetType()

		_, err := typesys.GetRelation(subjectType, computedRelation)
		if err != nil {
			if errors.Is(err, typesystem.ErrRelationUndefined) {
				continue
			}

			// todo: handle error
			panic(err)
		}

		subjectTypes = append(subjectTypes, subjectType)
		quotedSubjectTypes = append(quotedSubjectTypes, fmt.Sprintf(`'%s'`, subjectType))
	}

	sql := fmt.Sprintf(`SELECT subject_type, subject_id, subject_relation, relation, object_type, object_id
    FROM tuples
    WHERE subject_type IN (%s) AND relation = '%s' AND object_type = '%s' UNION `, strings.Join(quotedSubjectTypes, ","), tuplesetRelation, objectType)

	for i, subjectType := range subjectTypes {
		if i < len(subjectTypes)-1 {
			sql += fmt.Sprintf(`SELECT i.subject_type, i.subject_id, i.subject_relation, '%s', p.object_type, p.object_id
			FROM %s p, %s i
			WHERE p.relation = '%s' AND p.object_type = '%s'
			AND p.subject_type = i.object_type AND p.subject_id = i.object_id
			AND i.relation = '%s' UNION `, computedRelation, fmt.Sprintf("%s_%s", objectType, relation), fmt.Sprintf("%s_%s", subjectType, computedRelation), tuplesetRelation, objectType, computedRelation)
		} else {
			sql += fmt.Sprintf(`SELECT i.subject_type, i.subject_id, i.subject_relation, '%s', p.object_type, p.object_id
			FROM %s p, %s i
			WHERE p.relation = '%s' AND p.object_type = '%s'
			AND p.subject_type = i.object_type AND p.subject_id = i.object_id
			AND i.relation = '%s'`, computedRelation, fmt.Sprintf("%s_%s", objectType, relation), fmt.Sprintf("%s_%s", subjectType, computedRelation), tuplesetRelation, objectType, computedRelation)
		}
	}

	return NamedSQLStatement{
		name: fmt.Sprintf("%s_%s", objectType, relation),
		sql:  sql,
	}
}
