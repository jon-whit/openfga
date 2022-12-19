package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/openfga/openfga/internal/dispatcher"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type setOperationType int

const (
	unionSetOperation setOperationType = iota
	intersectionSetOperation
	exclusionSetOperation
)

type checkOutcome struct {
	resp *openfgapb.CheckResponse
	err  error
}

// ConcurrentChecker implements Check in a highly concurrent and localized manner. The
// Check resolution is limited per branch of evaluation by the concurrencyLimit.
type ConcurrentChecker struct {
	ds               storage.OpenFGADatastore
	dispatcher       dispatcher.CheckDispatcher
	concurrencyLimit uint32
}

// NewConcurrentChecker constructs a ConcurrentChecker that can be used to evaluate a Check
// request locally and with a high degree of concurrency.
func NewConcurrentChecker(
	ds storage.OpenFGADatastore,
	concurrencyLimit uint32,
) *ConcurrentChecker {
	checker := &ConcurrentChecker{ds: ds, concurrencyLimit: concurrencyLimit}
	checker.dispatcher = checker // todo: replace with a different CheckDispatcher once we support dispatching

	return checker
}

// CheckHandlerFunc defines a function that evaluates a CheckResponse or returns an error
// otherwise.
type CheckHandlerFunc func(ctx context.Context) (*openfgapb.CheckResponse, error)

// CheckFuncReducer defines a function that combines or reduces one or more CheckHandlerFunc into
// a single CheckResponse with a maximum limit on the number of concurrent evaluations that can be
// in flight at any given time.
type CheckFuncReducer func(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error)

// resolver concurrently resolves one or more CheckHandlerFunc and yields the results on the provided resultChan.
// Callers of the 'resolver' function should be sure to invoke the callback returned from this function to ensure
// every concurrent check is evaluated. The concurrencyLimit can be set to provide a maximum number of concurrent
// evaluations in flight at any point.
func resolver(ctx context.Context, concurrencyLimit uint32, resultChan chan<- checkOutcome, handlers ...CheckHandlerFunc) func() {
	limiter := make(chan struct{}, concurrencyLimit)

	var wg sync.WaitGroup

	checker := func(fn CheckHandlerFunc) {
		resp, err := fn(ctx)
		resultChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}

	wg.Add(1)
	go func() {
		for _, handler := range handlers {
			fn := handler // capture loop var

			select {
			case limiter <- struct{}{}:
				wg.Add(1)
				go checker(fn)
			case <-ctx.Done():
				break
			}
		}

		wg.Done()
	}()

	return func() {
		wg.Wait()
		close(limiter)
	}
}

// union implements a CheckFuncReducer that requires any of the provided CheckHandlerFunc to resolve
// to an allowed outcome. The first allowed outcome causes premature termination of the reducer.
func union(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error) {

	cctx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(cctx, concurrencyLimit, resultChan, handlers...)

	defer func() {
		cancel()
		drain()
		close(resultChan)
	}()

	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, result.err
			}

			if result.resp.GetAllowed() {
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &openfgapb.CheckResponse{Allowed: false}, nil
}

// intersection implements a CheckFuncReducer that requires all of the provided CheckHandlerFunc to resolve
// to an allowed outcome. The first falsey or erroneous outcome causes premature termination of the reducer.
func intersection(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error) {

	cctx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(cctx, concurrencyLimit, resultChan, handlers...)

	defer func() {
		cancel()
		drain()
		close(resultChan)
	}()

	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:

			if result.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, result.err
			}

			if !result.resp.GetAllowed() {
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &openfgapb.CheckResponse{Allowed: true}, nil
}

// exclusion implements a CheckFuncReducer that requires a 'base' CheckHandlerFunc to resolve to an allowed
// outcome and a 'sub' CheckHandlerFunc to resolve to a falsey outcome. The base and sub computations are
// handled concurrently relative to one another.
func exclusion(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error) {

	if len(handlers) != 2 {
		panic(fmt.Sprintf("expected two rewrite operands for exclusion operator, but got '%d'", len(handlers)))
	}

	limiter := make(chan struct{}, concurrencyLimit)

	cctx, cancel := context.WithCancel(ctx)
	baseChan := make(chan checkOutcome, 1)
	subChan := make(chan checkOutcome, 1)

	var wg sync.WaitGroup

	defer func() {
		cancel()
		wg.Wait()
		close(baseChan)
		close(subChan)
	}()

	baseHandler := handlers[0]
	subHandler := handlers[1]

	limiter <- struct{}{}
	wg.Add(1)
	go func() {
		resp, err := baseHandler(cctx)
		baseChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}()

	limiter <- struct{}{}
	wg.Add(1)
	go func() {
		resp, err := subHandler(cctx)
		subChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}()

	for i := 0; i < len(handlers); i++ {
		select {
		case baseResult := <-baseChan:
			if baseResult.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, baseResult.err
			}

			if !baseResult.resp.Allowed {
				return &openfgapb.CheckResponse{Allowed: false}, nil
			}
		case subResult := <-subChan:
			if subResult.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, subResult.err
			}

			if subResult.resp.Allowed {
				return &openfgapb.CheckResponse{Allowed: false}, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &openfgapb.CheckResponse{Allowed: true}, nil
}

// dispatch dispatches the provided Check request to the CheckDispatcher this ConcurrentChecker
// was constructed with.
func (c *ConcurrentChecker) dispatch(ctx context.Context, req *dispatcher.DispatchCheckRequest) CheckHandlerFunc {
	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {
		resp, err := c.dispatcher.DispatchCheck(ctx, req)
		if err != nil {
			return nil, err
		}

		return &openfgapb.CheckResponse{
			Allowed: resp.Allowed,
		}, nil
	}
}

func (c *ConcurrentChecker) DispatchCheck(
	ctx context.Context,
	req *dispatcher.DispatchCheckRequest,
) (*dispatcher.DispatchCheckResponse, error) {

	if req.GetResolutionMetadata().Depth == 0 {
		return nil, fmt.Errorf("resolution depth exceeded")
	}

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		panic("typesystem missing in context")
	}

	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	object := req.GetTupleKey().GetObject()
	relation := req.GetTupleKey().GetRelation()

	objectType, _ := tuple.SplitObject(object)
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		return nil, fmt.Errorf("relation '%s' undefined for object type '%s'", relation, objectType)
	}

	resp, err := union(ctx, c.concurrencyLimit, c.checkRewrite(ctx, req, rel.GetRewrite()))
	if err != nil {
		return nil, err
	}

	return &dispatcher.DispatchCheckResponse{
		Allowed: resp.Allowed,
	}, nil
}

// checkDirect composes two CheckHandlerFunc which evaluate direct relationships with the provided
// 'object#relation'. The first handler looks up direct matches on the provided 'object#relation@user',
// while the second handler looks up relationships between the target 'object#relation' and any usersets
// related to it.
func (c *ConcurrentChecker) checkDirect(parentctx context.Context, req *dispatcher.DispatchCheckRequest) CheckHandlerFunc {
	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {

		storeID := req.GetStoreId()
		tk := req.GetTupleKey()

		fn1 := func(ctx context.Context) (*openfgapb.CheckResponse, error) {
			t, err := c.ds.ReadUserTuple(ctx, storeID, tk)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return &openfgapb.CheckResponse{Allowed: false}, nil
				}

				return &openfgapb.CheckResponse{Allowed: false}, err
			}

			if t != nil {
				return &openfgapb.CheckResponse{Allowed: true}, nil
			}

			return &openfgapb.CheckResponse{Allowed: false}, nil
		}

		fn2 := func(ctx context.Context) (*openfgapb.CheckResponse, error) {
			iter, err := c.ds.ReadUsersetTuples(ctx, storeID, tk)
			if err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, err
			}

			var handlers []CheckHandlerFunc
			for {
				t, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}

					return &openfgapb.CheckResponse{Allowed: false}, err
				}

				// otherwise, check the userset
				usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetKey().GetUser())

				handlers = append(handlers, c.dispatch(
					ctx,
					&dispatcher.DispatchCheckRequest{
						StoreId:              storeID,
						AuthorizationModelId: req.GetAuthorizationModelId(),
						TupleKey:             tuple.NewTupleKey(usersetObject, usersetRelation, tk.GetUser()),
						ResolutionMetadata: &dispatcher.ResolutionMetadata{
							Depth: req.GetResolutionMetadata().Depth - 1,
						},
					}))
			}

			if len(handlers) > 0 {
				return union(ctx, c.concurrencyLimit, handlers...)
			}

			return &openfgapb.CheckResponse{Allowed: false}, nil
		}

		return union(ctx, c.concurrencyLimit, fn1, fn2)
	}
}

// checkTTU looks up all tuples of the target tupleset relation on the provided object and for each one
// of them evaluates the computed userset of the TTU rewrite rule for them.
func (c *ConcurrentChecker) checkTTU(parentctx context.Context, req *dispatcher.DispatchCheckRequest, rewrite *openfgapb.Userset) CheckHandlerFunc {

	typesys, ok := typesystem.TypesystemFromContext(parentctx)
	if !ok {
		panic("typesystem missing in context")
	}

	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		tuplesetRelation := rewrite.GetTupleToUserset().GetTupleset().GetRelation()
		computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

		tk := req.GetTupleKey()

		iter, err := c.ds.Read(
			ctx,
			req.GetStoreId(),
			tuple.NewTupleKey(tk.GetObject(), tuplesetRelation, ""),
		)
		if err != nil {
			return &openfgapb.CheckResponse{Allowed: false}, err
		}
		defer iter.Stop()

		var handlers []CheckHandlerFunc
		for {
			t, err := iter.Next(ctx)
			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				return &openfgapb.CheckResponse{Allowed: false}, err
			}

			userObj, _ := tuple.SplitObjectRelation(t.GetKey().GetUser())

			tupleKey := &openfgapb.TupleKey{
				Object:   userObj,
				Relation: computedRelation,
				User:     tk.GetUser(),
			}

			handlers = append(handlers, c.dispatch(
				ctx,
				&dispatcher.DispatchCheckRequest{
					StoreId:              req.GetStoreId(),
					AuthorizationModelId: req.GetAuthorizationModelId(),
					TupleKey:             tupleKey,
					ResolutionMetadata: &dispatcher.ResolutionMetadata{
						Depth: req.GetResolutionMetadata().Depth - 1,
					},
				}))
		}

		if len(handlers) > 0 {
			return union(ctx, c.concurrencyLimit, handlers...)
		}

		return &openfgapb.CheckResponse{Allowed: false}, nil
	}
}

func (c *ConcurrentChecker) checkSetOperation(
	ctx context.Context,
	req *dispatcher.DispatchCheckRequest,
	setOpType setOperationType,
	reducer CheckFuncReducer,
	children ...*openfgapb.Userset,
) CheckHandlerFunc {

	var handlers []CheckHandlerFunc

	switch setOpType {
	case unionSetOperation, intersectionSetOperation:
		for _, child := range children {
			handlers = append(handlers, c.checkRewrite(ctx, req, child))
		}
	case exclusionSetOperation:
		if len(children) != 2 {
			panic(fmt.Sprintf("expected two rewrite operands in exclusion operator, but got '%d'", len(children)))
		}

		handlers = append(handlers,
			c.checkRewrite(ctx, req, children[0]),
			c.checkRewrite(ctx, req, children[1]),
		)
	}

	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {
		return reducer(ctx, c.concurrencyLimit, handlers...)
	}
}

func (c *ConcurrentChecker) checkRewrite(
	ctx context.Context,
	req *dispatcher.DispatchCheckRequest,
	rewrite *openfgapb.Userset,
) CheckHandlerFunc {

	switch rw := rewrite.Userset.(type) {
	case *openfgapb.Userset_This:
		return c.checkDirect(ctx, req)
	case *openfgapb.Userset_ComputedUserset:

		return c.dispatch(
			ctx,
			&dispatcher.DispatchCheckRequest{
				StoreId:              req.GetStoreId(),
				AuthorizationModelId: req.GetAuthorizationModelId(),
				TupleKey: tuple.NewTupleKey(
					req.TupleKey.GetObject(),
					rw.ComputedUserset.GetRelation(),
					req.TupleKey.GetUser(),
				),
				ResolutionMetadata: &dispatcher.ResolutionMetadata{
					Depth: req.ResolutionMetadata.Depth - 1,
				},
			})

	case *openfgapb.Userset_TupleToUserset:
		return c.checkTTU(ctx, req, rewrite)
	case *openfgapb.Userset_Union:
		return c.checkSetOperation(ctx, req, unionSetOperation, union, rw.Union.GetChild()...)
	case *openfgapb.Userset_Intersection:
		return c.checkSetOperation(ctx, req, intersectionSetOperation, intersection, rw.Intersection.GetChild()...)
	case *openfgapb.Userset_Difference:
		return c.checkSetOperation(ctx, req, exclusionSetOperation, exclusion, rw.Difference.GetBase(), rw.Difference.GetSubtract())
	default:
		panic("unexpected userset rewrite encountere")
	}
}