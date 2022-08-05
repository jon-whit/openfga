package server

import (
	"context"
	"errors"
	"os"
	"path"
	"runtime"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/gateway"
	"github.com/openfga/openfga/server/test"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	"github.com/openfga/openfga/storage/postgres"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func TestOpenFGAServer(t *testing.T) {

	t.Run("TestPostgresDatastore", func(t *testing.T) {
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "postgres")

		test.TestAll(t, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
				ds, err := postgres.NewPostgresDatastore(uri)
				require.NoError(t, err)

				return ds
			})

			return ds, nil
		}))
	})

	t.Run("TestMemoryDatastore", func(t *testing.T) {
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "memory")

		test.TestAll(t, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
				return memory.New(telemetry.NewNoopTracer(), 10, 24)
			})

			return ds, nil
		}))
	})
}

func BenchmarkOpenFGAServer(b *testing.B) {

	b.Run("TestPostgresDatastore", func(b *testing.B) {
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(b, "postgres")

		test.BenchmarkAll(b, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(b, func(engine, uri string) storage.OpenFGADatastore {
				ds, err := postgres.NewPostgresDatastore(uri)
				require.NoError(b, err)

				return ds
			})

			return ds, nil
		}))
	})

	b.Run("TestMemoryDatastore", func(b *testing.B) {
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(b, "memory")

		test.BenchmarkAll(b, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(b, func(engine, uri string) storage.OpenFGADatastore {
				return memory.New(telemetry.NewNoopTracer(), 10, 24)
			})

			return ds, nil
		}))
	})
}

func TestIntersection(t *testing.T) {
	a := []string{"a"}
	b := []string{"a", "b"}
	c := []string{"a", "c"}

	require.Equal(t, []string{"a"}, intersect(a, b, c))

	a = []string{"a", "b"}
	b = []string{"a"}

	require.Equal(t, []string{"a"}, intersection(a, b))
}

func TestExpandUsers(t *testing.T) {

	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()

	testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "memory")
	datastore := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
		return memory.New(telemetry.NewNoopTracer(), 10, 24)
	})

	store := "store1"
	_, err := datastore.CreateStore(context.Background(), &openfgav1.Store{
		Id: store,
	})
	require.NoError(t, err)

	modelID, err := id.NewString()
	require.NoError(t, err)

	err = datastore.WriteAuthorizationModel(context.Background(), store, modelID, &openfgav1.TypeDefinitions{
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "group",
				Relations: map[string]*openfgav1.Userset{
					"member": {
						Userset: &openfgav1.Userset_This{},
					},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgav1.Userset{
					"owner": {
						Userset: &openfgav1.Userset_This{},
					},
					"member": {
						Userset: &openfgav1.Userset_Union{
							Union: &openfgav1.Usersets{
								Child: []*openfgav1.Userset{
									{
										Userset: &openfgav1.Userset_ComputedUserset{

											ComputedUserset: &openfgav1.ObjectRelation{
												Relation: "owner",
											},
										},
									},
									{
										Userset: &openfgav1.Userset_This{},
									},
								},
							},
						},
					},
					"coordinator": {
						Userset: &openfgav1.Userset_Union{
							Union: &openfgav1.Usersets{
								Child: []*openfgav1.Userset{
									{
										Userset: &openfgav1.Userset_This{},
									},
									{
										Userset: &openfgav1.Userset_ComputedUserset{
											ComputedUserset: &openfgav1.ObjectRelation{
												Relation: "owner",
											},
										},
									},
								},
							},
						},
					},
					"ambassador": {
						Userset: &openfgav1.Userset_Difference{
							Difference: &openfgav1.Difference{
								Base: &openfgav1.Userset{
									Userset: &openfgav1.Userset_ComputedUserset{
										ComputedUserset: &openfgav1.ObjectRelation{
											Relation: "member",
										},
									},
								},
								Subtract: &openfgav1.Userset{
									Userset: &openfgav1.Userset_ComputedUserset{
										ComputedUserset: &openfgav1.ObjectRelation{
											Relation: "limited",
										},
									},
								},
							},
						},
					},
					"limited": {
						Userset: &openfgav1.Userset_This{},
					},
					"seller": {
						Userset: &openfgav1.Userset_Intersection{
							Intersection: &openfgav1.Usersets{
								Child: []*openfgav1.Userset{
									{
										Userset: &openfgav1.Userset_ComputedUserset{
											ComputedUserset: &openfgav1.ObjectRelation{
												Relation: "owner",
											},
										},
									},
									{
										Userset: &openfgav1.Userset_ComputedUserset{
											ComputedUserset: &openfgav1.ObjectRelation{
												Relation: "approved",
											},
										},
									},
								},
							},
						},
					},
					"approved": {
						Userset: &openfgav1.Userset_This{},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	datastore.Write(context.Background(), store, nil, []*openfgav1.TupleKey{
		{
			Object:   "group:engineering",
			Relation: "member",
			User:     "group:fga#member",
		},
		{
			Object:   "group:engineering",
			Relation: "member",
			User:     "vittorio",
		},
		{
			Object:   "group:fga",
			Relation: "member",
			User:     "jon",
		},
		{
			Object:   "group:fga",
			Relation: "member",
			User:     "andres",
		},
		{
			Object:   "team:jazz",
			Relation: "owner",
			User:     "larry",
		},
		{
			Object:   "team:jazz",
			Relation: "owner",
			User:     "tim",
		},
		{
			Object:   "team:jazz",
			Relation: "approved",
			User:     "larry",
		},
		{
			Object:   "team:jazz",
			Relation: "coordinator",
			User:     "jon",
		},
		{
			Object:   "team:jazz",
			Relation: "member",
			User:     "jill",
		},
		{
			Object:   "team:jazz",
			Relation: "limited",
			User:     "jill",
		},
	})

	s := Server{
		datastore: datastore,
		tracer:    tracer,
		transport: transport,
		logger:    logger,
	}

	users, err := s.expandUsers(store, modelID, "group:engineering#member")
	require.NoError(t, err)

	sort.Slice(users, func(i, j int) bool {
		if users[i] <= users[j] {
			return true
		}
		return false
	})

	expected := []string{"vittorio", "jon", "andres"}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i] <= expected[j] {
			return true
		}
		return false
	})

	require.Equal(t, expected, users)

	users, err = s.expandUsers(store, modelID, "team:jazz#member")
	require.NoError(t, err)

	sort.Slice(users, func(i, j int) bool {
		if users[i] <= users[j] {
			return true
		}
		return false
	})

	expected = []string{"larry", "jill", "tim"}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i] <= expected[j] {
			return true
		}
		return false
	})

	require.Equal(t, expected, users)

	users, err = s.expandUsers(store, modelID, "team:jazz#coordinator")
	require.NoError(t, err)

	sort.Slice(users, func(i, j int) bool {
		if users[i] <= users[j] {
			return true
		}
		return false
	})

	expected = []string{"larry", "tim", "jon"}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i] <= expected[j] {
			return true
		}
		return false
	})

	require.Equal(t, expected, users)

	users, err = s.expandUsers(store, modelID, "team:jazz#ambassador")
	require.NoError(t, err)

	require.Equal(t, []string{"larry", "tim"}, users)

	users, err = s.expandUsers(store, modelID, "team:jazz#seller")
	require.NoError(t, err)

	require.Equal(t, []string{"larry"}, users)
}

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()

	t.Run("no latest authorization model id found", func(t *testing.T) {

		store := testutils.CreateRandomString(10)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		s := Server{
			datastore: mockDatastore,
			tracer:    tracer,
			transport: transport,
			logger:    logger,
		}

		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		if _, err := s.resolveAuthorizationModelID(ctx, store, ""); !errors.Is(err, expectedError) {
			t.Errorf("Expected '%v' but got %v", expectedError, err)
		}
	})

	t.Run("read existing authorization model", func(t *testing.T) {
		store := testutils.CreateRandomString(10)

		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return(modelID, nil)

		s := Server{
			datastore: mockDatastore,
			tracer:    tracer,
			transport: transport,
			logger:    logger,
		}

		got, err := s.resolveAuthorizationModelID(ctx, store, "")
		if err != nil {
			t.Fatal(err)
		}
		if got != modelID {
			t.Errorf("wanted '%v', but got %v", modelID, got)
		}
	})

	t.Run("non-valid modelID returns error", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		modelID := "foo"
		want := serverErrors.AuthorizationModelNotFound(modelID)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := Server{
			datastore: mockDatastore,
			tracer:    tracer,
			transport: transport,
			logger:    logger,
		}

		if _, err := s.resolveAuthorizationModelID(ctx, store, modelID); err.Error() != want.Error() {
			t.Fatalf("got '%v', want '%v'", err, want)
		}
	})
}
