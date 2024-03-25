package index

import (
	"context"
	"fmt"
	"os"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/spf13/cobra"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/typesystem"
)

type materializations struct {
	views      []string
	statements []string
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

	flags.String("object-type", "", "the object type to prodouce an index materialization for (e.g. document)")

	flags.String("relation", "", "the relation to produce an index materialization for (e.g. viewer)")

	flags.String("user-type", "", "the type of the user/subject to produce an index materialization for (e.g. group)")
	flags.String("user-relation", "", "the type of the user/subject to produce an index materialization for (e.g. member)")

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

	userType, err := cmd.Flags().GetString("user-type")
	if err != nil {
		panic("'user-type' is a required flag")
	}

	userRelation, err := cmd.Flags().GetString("user-relation")
	if err != nil {
		panic("failed to get 'user-relation' flag")
	}

	relation, err := cmd.Flags().GetString("relation")
	if err != nil {
		panic("'relation' is a required flag")
	}

	objectType, err := cmd.Flags().GetString("object-type")
	if err != nil {
		panic("'object-type' is a required flag")
	}

	statements := materialize(materializationInput{
		indexName,
		modelFile,
		userType,
		userRelation,
		relation,
		objectType,
	})

	_ = statements
	// sqlfmt the statements and print them out
}

type materializationInput struct {
	indexName    string
	modelFile    string
	userType     string
	userRelation string
	relation     string
	objectType   string
}

// materialize produces one or more statements defining the materialized views that
// materialize some FGA index.
func materialize(in materializationInput) []string {
	modelBytes, err := os.ReadFile(in.modelFile)
	if err != nil {
		panic(fmt.Sprintf("model file error: %v", err))
	}

	model := parser.MustTransformDSLToProto(string(modelBytes))

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	if err != nil {
		panic(err)
	}

	materializations := materializeInternal(graph.New(typesys), in)

	fmt.Println(materializations.views)
	return materializations.views
}

func materializeInternal(
	g *graph.RelationshipGraph,
	in materializationInput,
) materializations {
	objectType := in.objectType
	relation := in.relation

	typesys := g.GetTypesystem()

	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		panic(err)
	}

	switch rewrite := rel.GetRewrite().GetUserset().(type) {
	case *openfgav1.Userset_This:
		materializations := materializeDirectRelationships(g, in)

		statement := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW %s AS (

			UNION	
		);`, in.indexName)

		materializations.views = append(materializations.views, statement)

		return materializations
	default:
		_ = rewrite
		panic("unsupported relationship rewrite provided")
	}
}

func materializeDirectRelationships(
	g *graph.RelationshipGraph,
	in materializationInput,
) materializations {
	objectType := in.objectType
	relation := in.relation
	userType := in.userType
	userRelation := in.userRelation

	sourceUserRef := &openfgav1.RelationReference{
		Type: userType,
	}

	if userRelation != "" {
		sourceUserRef.RelationOrWildcard = &openfgav1.RelationReference_Relation{
			Relation: userRelation,
		}
	}

	edges, err := g.GetRelationshipEdges(
		&openfgav1.RelationReference{
			Type: objectType,
			RelationOrWildcard: &openfgav1.RelationReference_Relation{
				Relation: relation,
			},
		},
		sourceUserRef,
	)
	if err != nil {
		panic(err)
	}

	if len(edges) < 1 {
		panic("no indexable relationships - no entrypoints")
	}

	materializations := materializations{
		views:      []string{},
		statements: []string{},
	}

	if userType == objectType && userRelation == relation {
		viewName := fmt.Sprintf("%s_%s_%s_%s", objectType, relation, userType, userRelation)
		statement := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW %s AS
		WITH MUTUALLY RECURSIVE
		%s(object_type TEXT, object_id TEXT, relation TEXT, subject_object_type TEXT, subject_object_id TEXT, subject_relation TEXT) AS (
			SELECT DISTINCT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation FROM relationship_tuples WHERE object_type='%s' AND relation='%s' AND subject_object_type='%s' AND subject_relation='%s'

			UNION ALL

			SELECT DISTINCT a2.object_type, a2.object_id, a2.relation, a1.subject_object_type, a1.subject_object_id, a1.subject_relation FROM %s a1 JOIN %s a2 ON a1.object_id = a2.subject_object_id
		)
		SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation FROM %s;
		`, viewName, viewName, viewName, objectType, relation, userType, userRelation, viewName, viewName)

		materializations.views = append(materializations.views, statement)

		return materializations
	}

	for _, edge := range edges {
		switch edge.Type {
		case graph.DirectEdge:
			targetRef := edge.TargetReference
			targetObjectType := targetRef.GetType()
			targetRelation := targetRef.GetRelation()

			var viewName string
			if userRelation != "" {
				viewName = fmt.Sprintf("%s_%s_%s_%s", targetObjectType, targetRelation, userType, userRelation)
			} else {
				viewName = fmt.Sprintf("%s_%s_%s", targetObjectType, targetRelation, userType)
			}

			statement := fmt.Sprintf(`
			CREATE MATERIALIZED VIEW %s AS (
				SELECT object_id, subject_object_id FROM relationship_tuples
				WHERE
				object_type='%s' AND relation='%s' AND subject_object_type='%s' AND subject_relation='%s'
			  );
			`, viewName, targetObjectType, targetRelation, userType, userRelation)

			materializations.views = append(materializations.views, statement)

			m := materializeDirectRelationships(g, materializationInput{
				objectType:   objectType,
				relation:     relation,
				userType:     targetObjectType,
				userRelation: targetRelation,
			})

			materializations.views = append(materializations.views, m.views...)
			materializations.statements = append(materializations.statements, m.statements...)

		default:
			panic("unsupported RelationEdge type - only DirectEdge supported at this time")
		}
	}

	return materializations
}
