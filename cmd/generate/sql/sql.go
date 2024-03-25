package sql

import (
	"context"
	"fmt"
	"os"

	"github.com/go-jet/jet/v2/postgres"
	. "github.com/go-jet/jet/v2/postgres"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/spf13/cobra"

	. "github.com/openfga/openfga/gen/postgres/public/table"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/typesystem"
)

type RelationshipTuple struct {
	ObjectType      string
	ObjectID        string
	Relation        string
	SubjectType     string
	SubjectID       string
	SubjectRelation string
}

func NewGenerateSQLCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql",
		Short: "FGA SQL generator",
		Run:   generateSQL,
		Args:  cobra.NoArgs,
	}

	flags := cmd.Flags()

	flags.String("file", "model.fga", "an absolute file path to an FGA model (default 'model.fga')")

	flags.String("object-type", "", "the object type to prodouce the SQL query for (e.g. document)")

	flags.String("relation", "", "the relation to prodouce the SQL query for (e.g. viewer)")

	return cmd
}

func generateSQL(cmd *cobra.Command, args []string) {
	modelFile, err := cmd.Flags().GetString("file")
	if err != nil {
		panic("'file' is a required flag")
	}

	relation, err := cmd.Flags().GetString("relation")
	if err != nil {
		panic("'relation' is a required flag")
	}

	objectType, err := cmd.Flags().GetString("object-type")
	if err != nil {
		panic("'object-type' is a required flag")
	}

	sql := genSQL(generateSQLInput{
		modelFile,
		relation,
		objectType,
	})

	fmt.Println(sql)
}

type generateSQLInput struct {
	modelFile  string
	relation   string
	objectType string
}

// genSQL produces the SQL statement that represents the flattened query
// for the given FGA relationship.
func genSQL(in generateSQLInput) string {
	modelBytes, err := os.ReadFile(in.modelFile)
	if err != nil {
		panic(fmt.Sprintf("model file error: %v", err))
	}

	model := parser.MustTransformDSLToProto(string(modelBytes))

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	if err != nil {
		panic(err)
	}

	stmt := sqlInternal(graph.New(typesys), in)
	sql, _ := stmt.Sql()
	return sql
}

const (
	tableName = "relationship_tuples"
)

func sqlInternal_This(objectType, relation string) postgres.SelectStatement {
	stmt := SELECT(
		RelationshipTuples.ObjectType.AS(objectType),
		RelationshipTuples.ObjectID,
		RelationshipTuples.Relation,
		RelationshipTuples.SubjectObjectType,
		RelationshipTuples.SubjectObjectID,
		RelationshipTuples.SubjectRelation,
	).FROM(RelationshipTuples).
		WHERE(
			RelationshipTuples.ObjectType.EQ(String(objectType)).
				AND(
					RelationshipTuples.Relation.EQ(String(relation)),
				),
		)
	return stmt

	// return fmt.Sprintf(`
	// SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
	// FROM %s
	// WHERE object_type='%s' AND relation='%s'`, objectType, relation, tableName, objectType, relation)
}

func sqlInternal(
	g *graph.RelationshipGraph,
	in generateSQLInput,
) postgres.Statement {
	objectType := in.objectType
	relation := in.relation

	typesys := g.GetTypesystem()

	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		panic(err)
	}

	switch rewrite := rel.GetRewrite().GetUserset().(type) {
	case *openfgav1.Userset_This:
		cte := CTE(fmt.Sprintf("%s_%s_this", objectType, relation))
		stmt := WITH(
			cte.AS(
				SELECT(
					RelationshipTuples.ObjectType.AS(objectType),
					RelationshipTuples.ObjectID,
					RelationshipTuples.Relation.AS(relation),
					RelationshipTuples.SubjectObjectType,
					RelationshipTuples.SubjectObjectID,
					RelationshipTuples.SubjectRelation,
				).FROM(RelationshipTuples).
					WHERE(
						RelationshipTuples.ObjectType.EQ(String(objectType)).
							AND(
								RelationshipTuples.Relation.EQ(String(relation)),
							),
					),
			),
		)(
			SELECT(
				RelationshipTuples.ObjectType.AS(objectType),
				RelationshipTuples.ObjectID,
				RelationshipTuples.Relation.AS(relation),
				RelationshipTuples.SubjectObjectType,
				RelationshipTuples.SubjectObjectID,
				RelationshipTuples.SubjectRelation,
			).FROM(cte),
		)

		return stmt

		// 		expressionName := fmt.Sprintf(`%s_%s_this`, objectType, relation)

		// 		expression := fmt.Sprintf(`
		// WITH %s AS (
		// 	SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
		// 	FROM %s
		// 	WHERE object_type='%s' AND relation='%s'
		// )
		// SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
		// FROM %s`, expressionName, objectType, relation, tableName, objectType, relation, objectType, relation, expressionName)

		// 		return expression
	case *openfgav1.Userset_Union:
		/*
			WITH (
				... expressions
			)

			SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
			FROM (
				SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
				FROM expressions[0].name

				UNION


				SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
				FROM expressions[1].name

				UNION

				...
			) AS {objectType_relation}
		*/

		// expression := `WITH `

		// expressionNames := []string{}

		var statements []postgres.SelectStatement
		childRewrites := rewrite.Union.GetChild()
		for _, child := range childRewrites {
			switch childRewrite := child.GetUserset().(type) {
			case *openfgav1.Userset_This:
				statements = append(statements, sqlInternal_This(objectType, relation))

				//innerCTEs = append(innerCTEs, sqlInternal_This(objectType, relation))
				// 				expressionName := fmt.Sprintf("%s_%s_this", objectType, relation)

				// 				expressionNames = append(expressionNames, expressionName)

				// 				if i == len(childRewrites)-1 {
				// 					expression += fmt.Sprintf(`%s AS (
				// 	%s
				// )`, expressionName, sqlInternal_This(objectType, relation))
				// 				} else {
				// 					expression += fmt.Sprintf(`%s AS (
				// 	%s
				// ),`, expressionName, sqlInternal_This(objectType, relation))
				// 				}
			case *openfgav1.Userset_ComputedUserset:
				computedRelation := childRewrite.ComputedUserset.GetRelation()

				statementName := fmt.Sprintf("%s_%s", objectType, computedRelation)
				computedCTE := CTE(statementName)

				stmt := WITH(
					computedCTE.AS(
						SELECT(
							RelationshipTuples.ObjectType.AS(objectType),
							RelationshipTuples.ObjectID,
							RelationshipTuples.Relation.AS(relation),
							RelationshipTuples.SubjectObjectType,
							RelationshipTuples.SubjectObjectID,
							RelationshipTuples.SubjectRelation,
						).WHERE(
							RelationshipTuples.ObjectType.EQ(String(objectType)).AND(
								RelationshipTuples.Relation.EQ(String(computedRelation)),
							),
						),
					),
				)(
					SELECT(
						RelationshipTuples.ObjectType.AS(objectType),
						RelationshipTuples.ObjectID,
						RelationshipTuples.Relation.AS(relation),
						RelationshipTuples.SubjectObjectType,
						RelationshipTuples.SubjectObjectID,
						RelationshipTuples.SubjectRelation,
					).
						FROM(computedCTE),
				)
				statements = append(statements, stmt)

				// expressionName := fmt.Sprintf("%s_%s", objectType, computedRelation)

				// expressionNames = append(expressionNames, expressionName)

				// 				if i == len(childRewrites)-1 {
				// 					expression += fmt.Sprintf(`%s AS (
				// 	%s
				// )`, expressionName, sqlInternal(g, generateSQLInput{
				// 						modelFile:  in.modelFile,
				// 						objectType: objectType,
				// 						relation:   computedRelation,
				// 					}))
				// 				} else {
				// 					expression += fmt.Sprintf(`%s AS (
				// 	%s
				// ),`, expressionName, sqlInternal(g, generateSQLInput{
				// 						modelFile:  in.modelFile,
				// 						objectType: objectType,
				// 						relation:   computedRelation,
				// 					}))
				// 				}

			default:
				_ = childRewrite
				panic("nested child rewrite not supported in union")
			}
		}

		cte := CTE(fmt.Sprintf("%s_%s", objectType, relation))

		innerStmt := statements[0]
		for i := 0; i < len(statements)-1; i++ {
			innerStmt.UNION(statements[i+1])
		}

		WITH(
			cte.AS(innerStmt),
		)(
			SELECT(
				RelationshipTuples.ObjectType.AS(objectType),
				RelationshipTuples.ObjectID,
				RelationshipTuples.Relation.AS(relation),
				RelationshipTuples.SubjectObjectType,
				RelationshipTuples.SubjectObjectID,
				RelationshipTuples.SubjectRelation,
			).FROM(cte),
		)

		// 		expression += fmt.Sprintf(`
		// 		SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
		// 		FROM (
		// 	`, objectType, relation)

		// 		if len(childRewrites)-1 == 0 {
		// 			expression += fmt.Sprintf(`
		// 		SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
		// 		FROM %s
		// 	)
		// 	`, objectType, relation, expressionNames[0])

		// 			return expression
		// 		}

		// 		for i := 0; i < len(childRewrites)-1; i++ {
		// 			expression += fmt.Sprintf(`
		// 		SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
		// 		FROM %s

		// 		UNION
		// 		`, objectType, relation, expressionNames[i])
		// 		}

		// 		expression += fmt.Sprintf(`
		// 		SELECT '%s' AS object_type, object_id, '%s' AS relation, subject_object_type, subject_object_id, subject_relation
		// 		FROM %s`, objectType, relation, expressionNames[len(childRewrites)-1])
		// 		expression += fmt.Sprintf(`
		// ) AS %s`, fmt.Sprintf("%s_%s", objectType, relation))

		// 		return expression
	case *openfgav1.Userset_Intersection:
		/*
			WITH (
				... expressions
			)

			SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
			FROM (
				SELECT expressions[0].name.object_type, expressions[0].name.object_id, '<relation>' AS relation, expressions[0].name.subject_object_type, expressions[0].name.subject_object_id, expressions[0].name.subject_relation
				FROM {expressions[0].name}
				INNER JOIN {expressions[1].name}
				ON {expressions[0].name}.object_id={expressions[1].name}.object_id
				INNER JOIN {expressions[2].name}
				ON {expressions[1].name}.object_id={expressions[2].name}.object_id;

				...
			) AS {objectType_relation}
		*/
	case *openfgav1.Userset_Difference:
		/*
			WITH (
				... expressions
			)

			SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
			FROM (
				SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
				FROM {expressions[0].name}

				EXCEPT SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
				FROM {expressions[1].name}

				EXCEPT SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
				FROM {expressions[2].name}

				etc..
			) AS {objectType_relation}
		*/
	case *openfgav1.Userset_ComputedUserset:
		/*
			WITH {objectType_computedRelation} AS (
				... expressions

			    WITH document_editor_this AS (
			        SELECT 'document' AS object_type, object_id, 'editor' AS relation, subject_object_type, subject_object_id, subject_relation
			        FROM relationship_tuples
			        WHERE object_type='document' AND relation='editor'
			    )
			    SELECT 'document' AS object_type, object_id, 'editor' AS relation, subject_object_type, subject_object_id, subject_relation
			    FROM document_editor_this
			)

			SELECT object_type, object_id, '<relation>' AS relation, subject_object_type, subject_object_id, subject_relation
			FROM (
			    SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation
			    FROM {objectType_computedRelation}
			) AS {objectType_relation};
		*/
	default:
		_ = rewrite
		panic("unsupported relationship rewrite provided")
	}

	return nil
}
