package indexer

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/tuple"
)

type indexerServer struct {
	openfgav1.UnimplementedIndexerServiceServer
	openfgaClient openfgav1.OpenFGAServiceClient
}

type IndexerServerOption func(s *indexerServer)

func WithOpenFGAClient(c openfgav1.OpenFGAServiceClient) IndexerServerOption {
	return func(s *indexerServer) {
		s.openfgaClient = c
	}
}

func NewIndexerServerWithsOpts(opts ...IndexerServerOption) *indexerServer {
	s := &indexerServer{}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *indexerServer) ExpandedReadChanges(
	req *openfgav1.ExpandedReadChangesRequest,
	srv openfgav1.IndexerService_ExpandedReadChangesServer,
) error {

	ctx := srv.Context()

	contToken := req.GetContinuationToken()
	for {
		readChangesResp, err := s.openfgaClient.ReadChanges(ctx, &openfgav1.ReadChangesRequest{
			StoreId:           req.GetStoreId(),
			ContinuationToken: contToken,
		})
		if err != nil {
			return err
		}

		changes := readChangesResp.GetChanges()
		contToken = readChangesResp.GetContinuationToken()

		for _, change := range changes {
			tupleKey := change.GetTupleKey()

			object := tupleKey.GetObject()
			relation := tupleKey.GetRelation()

			user := tupleKey.GetUser()
			userObjectStr, userRelation := tuple.SplitObjectRelation(user)
			userObjectType, userObjectID := tuple.SplitObject(userObjectStr)
			userObject := &openfgav1.Object{Type: userObjectType, Id: userObjectID}

			var contextualTuples []*openfgav1.TupleKey
			if change.Operation == openfgav1.TupleOperation_TUPLE_OPERATION_WRITE {
				fmt.Printf("(%s#%s@%s) written\n", object, relation, user)
			} else {
				fmt.Printf("(%s#%s@%s) deleted\n", object, relation, user)
				contextualTuples = []*openfgav1.TupleKey{
					tupleKey,
				}
			}

			var objects []*openfgav1.Object
			stream, err := s.openfgaClient.StreamedListObjects(ctx, &openfgav1.StreamedListObjectsRequest{
				StoreId:              req.GetStoreId(),
				AuthorizationModelId: req.GetAuthorizationModelId(),
				Type:                 req.GetTargetObjectType(),
				Relation:             req.GetRelation(),
				User:                 fmt.Sprintf("%s#%s", object, relation),
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: contextualTuples,
				},
			})
			if err != nil {
				log.Printf("failed to call StreamedListObjects: %v", err)
				return err
			}

			for {
				listObjectsResp, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					log.Printf("stream.Recv returned an error :%v", err)
					return err
				}

				object := listObjectsResp.GetObject()
				objectType, objectID := tuple.SplitObject(object)
				objects = append(objects, &openfgav1.Object{Type: objectType, Id: objectID})
			}

			fmt.Printf("potentially impacted objects: %v\n", objects)

			var subjects []*openfgav1.Object
			if userRelation != "" {
				subjectStream, err := s.openfgaClient.StreamedListUsers(ctx, &openfgav1.StreamedListUsersRequest{
					StoreId:              req.GetStoreId(),
					AuthorizationModelId: req.GetAuthorizationModelId(),
					Object:               userObject,
					Relation:             userRelation,
					TargetUserObjectType: req.GetTargetUserObjectType(),
					ContextualTuples:     contextualTuples,
				})
				if err != nil {
					return err
				}

				for {
					listUsersResp, err := subjectStream.Recv()
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						log.Printf("subjectStream.Recv returned an error :%v", err)
						return err
					}

					subjects = append(subjects, listUsersResp.GetUserObject())
				}
			} else {
				subjects = []*openfgav1.Object{userObject}
			}

			fmt.Printf("potentially impacted subjects: %v\n", subjects)

			for _, object := range objects {
				for _, subject := range subjects {

					// todo(optimization): if the object was reached through a direct relationship (e.g. no intersection or exclusion involved), then we can elide this Check

					// todo(scalability): dispatch these Checks out to other Indexer server peers
					checkResp, err := s.openfgaClient.Check(ctx, &openfgav1.CheckRequest{
						StoreId:              req.GetStoreId(),
						AuthorizationModelId: req.GetAuthorizationModelId(),
						TupleKey:             tuple.NewTupleKey(tuple.ObjectKey(object), req.GetRelation(), tuple.ObjectKey(subject)),
					})
					if err != nil {
						return err
					}

					relationshipStatus := openfgav1.RelationshipUpdate_NO_RELATIONSHIP
					if checkResp.GetAllowed() {
						relationshipStatus = openfgav1.RelationshipUpdate_HAS_RELATIONSHIP
					}

					srv.Send(&openfgav1.ExpandedReadChangesResponse{
						Result: &openfgav1.ExpandedReadChangesResponse_Update{
							Update: &openfgav1.RelationshipUpdate{
								Object:             object,
								Relation:           req.GetRelation(),
								User:               subject,
								RelationshipStatus: relationshipStatus,
							},
						},
					})
				}
			}

			srv.Send(&openfgav1.ExpandedReadChangesResponse{
				Result: &openfgav1.ExpandedReadChangesResponse_TupleChangesProcessed{
					TupleChangesProcessed: &openfgav1.TupleChangesProcessed{
						ContinuationToken: contToken,
					},
				},
			})
		}

		time.Sleep(2 * time.Second)
	}
}
