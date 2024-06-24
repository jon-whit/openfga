package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestGenerateIndex_Functional(t *testing.T) {

	tests := []struct {
		name            string
		temporarilySkip string
		model           string
		tuples          []string
		objectType      string
		relations       []string
		subjectType     string
		subjectRelation string

		expectedIndex []string
	}{
		{
			name: "test-1",
			model: `
			type user

			type document
			  relations
			    define viewer: [user]
			`,
			tuples: []string{
				"document:1#viewer@user:jon",
				"document:1#editor@user:jon",
			},
			objectType:  "document",
			relations:   []string{"viewer"},
			subjectType: "user",
			expectedIndex: []string{
				"document:1#viewer@user:jon",
			},
		},
		{
			name: "test-2a",
			model: `
			type user
			type employee

			type document
			  relations
			    define viewer: [user, employee]
			`,
			tuples: []string{
				"document:1#viewer@user:jon",
				"document:2#employee@user:jon",
			},
			objectType:  "document",
			relations:   []string{"viewer"},
			subjectType: "user",
			expectedIndex: []string{
				"document:1#viewer@user:jon",
			},
		},
		{
			name: "test-2b",
			model: `
			type user
			type employee

			type document
			  relations
			    define viewer: [user, employee]
			`,
			tuples: []string{
				"document:1#viewer@user:jon",
				"document:2#viewer@employee:bob",
			},
			objectType:  "",
			relations:   []string{},
			subjectType: "",
			expectedIndex: []string{
				"document:1#viewer@user:jon",
				"document:2#viewer@employee:bob",
			},
		},
		{
			name: "test-3a",
			model: `
			type user

			type group
			  relations
			    define member: [user, group#member]
			`,
			tuples: []string{
				"group:eng#member@group:fga#member",
				"group:fga#member@group:fga-backend#member",
				"group:fga#member@group:fga-frontend#member",
				"group:fga-backend#member@user:will",
				"group:fga-backend#member@user:maria",
				"group:fga-frontend#member@user:victoria",
			},
			objectType:  "group",
			relations:   []string{"member"},
			subjectType: "user",
			expectedIndex: []string{
				"group:eng#member@user:will",
				"group:eng#member@user:maria",
				"group:eng#member@user:victoria",
				"group:fga#member@user:will",
				"group:fga#member@user:maria",
				"group:fga#member@user:victoria",
				"group:fga-backend#member@user:will",
				"group:fga-backend#member@user:maria",
				"group:fga-frontend#member@user:victoria",
			},
		},
		{
			name: "test-3b",
			model: `
			type user

			type document
			  relations
			    define editor: [user, document#viewer]
				define viewer: [user, document#editor]
			`,
			tuples: []string{
				"document:1#viewer@document:1#editor",
				"document:1#editor@document:1#viewer",
				"document:2#viewer@document:2#editor",
				"document:2#editor@user:jon",
				"document:3#editor@document:3#viewer",
				"document:3#viewer@user:will",
				"document:4#viewer@user:maria",
				"document:4#editor@user:raghd",
			},
			objectType:  "document",
			relations:   []string{"viewer", "editor"},
			subjectType: "user",
			expectedIndex: []string{
				"document:2#viewer@user:jon",
				"document:2#editor@user:jon",
				"document:3#editor@user:will",
				"document:3#viewer@user:will",
				"document:4#viewer@user:maria",
				"document:4#editor@user:raghd",
			},
		},
		{
			name: "test-4a",
			model: `
			type user

			type document
			  relations
			    define editor: [user]
			    define viewer: editor
			`,
			tuples: []string{
				"document:1#editor@user:jon",
				"document:2#editor@user:will",
				"document:3#viewer@user:maria",
			},
			objectType:  "document",
			relations:   []string{"viewer"},
			subjectType: "user",
			expectedIndex: []string{
				"document:1#viewer@user:jon",
				"document:2#viewer@user:will",
			},
		},
		{
			name: "test-4b",
			model: `
			type user

			type document
			  relations
			    define owner: [user]
			    define editor: [user, document#owner]
			    define viewer: editor
			`,
			tuples: []string{
				"document:1#editor@user:jon",
				"document:2#editor@document:3#owner",
				"document:3#owner@user:will",
				"document:4#editor@document:4#owner",
				"document:4#owner@user:maria",
			},
			objectType:  "document",
			relations:   []string{"viewer"},
			subjectType: "user",
			expectedIndex: []string{
				"document:1#viewer@user:jon",
				"document:2#viewer@user:will",
				"document:4#viewer@user:maria",
			},
		},
		{
			name: "test-5",
			model: `
			type user

			type group
			  relations
			    define member: [user, group#member]

			type org
			  relations
			    define viewer: [user, group#member]

			type project
			  relations
			    define editor: [user]

			type folder
			  relations
			    define viewer: [user]

			type document
			  relations
			    define parent: [folder, org, project]
			    define viewer: viewer from parent
			`,
			tuples: []string{
				"document:1#viewer@user:jon",
				"document:2#parent@folder:x",
				"document:2#parent@folder:y",
				"folder:x#viewer@user:maria",
				"folder:y#viewer@user:will",
				"group:eng#member@group:fga#member",
				"group:fga#member@user:jon",
				"org:acme#viewer@group:eng#member",
				"document:3#parent@org:acme",
				"document:4#parent@project:demo",
				"project:demo#viewer@user:raghd", // existing tuple but not part of the model
			},
			objectType:  "document",
			relations:   []string{"viewer"},
			subjectType: "user",
			expectedIndex: []string{
				"document:2#viewer@user:maria",
				"document:2#viewer@user:will",
				"document:3#viewer@user:jon",
			},
		},
	}

	for i, test := range tests {
		ds := memory.New()

		t.Run(test.name, func(t *testing.T) {
			storeID := ulid.Make().String()

			var tuples []*openfgav1.TupleKey
			for _, t := range test.tuples {
				tuples = append(tuples, tuple.TupleKeyFromString(t))
			}

			err := ds.Write(context.Background(), storeID, nil, tuples)
			require.NoError(t, err)

			model := testutils.MustTransformDSLToProtoWithID(test.model)

			typesys, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(t, err)

			sql := materialize(materializationInput{
				indexName:       fmt.Sprintf("myindex_%d", i),
				typesys:         typesys,
				objectType:      test.objectType,
				relations:       test.relations,
				subjectType:     test.subjectType,
				subjectRelation: test.subjectRelation,
			})

			_ = sql

		})
	}
}
