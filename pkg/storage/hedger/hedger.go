package hedger

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	hedgableRequestCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "openfga",
		Subsystem: "storage",
		Name:      "hedgable_request_count",
		Help:      "A counter counting the number of requests that may be hedged",
	})

	hedgedRequestCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "openfga",
		Subsystem: "storage",
		Name:      "hedged_request_count",
		Help:      "A counter counting the number of requests that were hedged",
	})
)

// QuantileApproximator defines an interface that can be implemented to provide an approximation of quantiles
// of a given distribution on.
type QuantileApproximator interface {

	// Add adds the value x with weight w to the distribution.
	Add(x float64, w float64)

	// Quantile computes an approximation of the q'th quantile of the accumulated distribution.
	Quantile(q float64) float64
}

type boundedQuantileApproximator struct {
	mu         sync.Mutex
	maxSamples uint32
	tdigests   []*tdigest.TDigest
}

var _ QuantileApproximator = (*boundedQuantileApproximator)(nil)

// NewBoundedQuantileApproximator constructs a QuantileApproximator that approximates
// quantiles with a maximum bound on the number of samples that are included in the
// approximation.
//
// The QuantileApproximator return internally uses a TDigest, which provides a highly
// accurate approximation for rank-based statistics such as quantiles.
func NewBoundedQuantileApproximator(
	maxSamples uint32,
) QuantileApproximator {

	maindigest := tdigest.NewWithCompression(1000)
	maindigest.Add(0.02, 1) // initial hedge threshold (20ms)

	return &boundedQuantileApproximator{
		maxSamples: maxSamples,
		tdigests: []*tdigest.TDigest{
			maindigest,                       // main digest
			tdigest.NewWithCompression(1000), // swap digest (for zero-copy swap when maxSamples is reached)
		},
	}
}

func (b *boundedQuantileApproximator) Add(x float64, w float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	maindigest := b.tdigests[0]
	swapdigest := b.tdigests[1]

	// if we'd exceeded our maxSamples, then zero-copy swap and restart
	// the digest at the oldest added sample (e.g. wrap around)
	if maindigest.Count() >= float64(b.maxSamples) {
		b.tdigests = b.tdigests[1:]
		maindigest.Reset()

		b.tdigests = append(b.tdigests, maindigest)
	}

	maindigest.Add(x, w)
	swapdigest.Add(x, w)
}

func (b *boundedQuantileApproximator) Quantile(q float64) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	maindigest := b.tdigests[0]
	return maindigest.Quantile(q)
}

// hedgedfunc is a function provided by an implementation than intends to hedge a request.
// Implementations should yield a value on the 'resolved' channel when the request being
// hedged resolves.
//
// Implementations should take care to close the 'resolved' channel. To guarantee that the
// the slowest resolver doesn't attempt to publish to a closed channel, the slowest resolver
// (between the original and hedged requests) should be responsible for closing the channel.
type hedgedFunc func(ctx context.Context, resolved chan<- struct{})

// hedgedFuncResolver defines an interface that can be used to provided hedged function
// resolution.
type hedgedFuncResolver func(ctx context.Context, hedgedfunc hedgedFunc)

// hedger provides a hedgedFuncResolver that ensures the hedged function is invoked
// if and only if the function takes longer than the computed quantile.  Whichever
// function is faster (e.g. the hedged or original) is observed and added to the
// quantile approximation.
func hedger(q QuantileApproximator, quantile float64) hedgedFuncResolver {

	return func(ctx context.Context, hedgedfunc hedgedFunc) {

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		resolved := make(chan struct{}, 1)

		quantileSec := q.Quantile(quantile)

		hedgableRequestCount.Inc()

		timer := time.NewTimer(time.Duration(quantileSec * float64(time.Second)))

		start := time.Now()
		go hedgedfunc(ctx, resolved)

		var duration time.Duration
		select {
		case <-resolved:
			duration = time.Since(start)
		case <-timer.C:
			// hedge the request if we've hit the target deadline
			hedgedRequestCount.Inc()

			hedgedResolved := make(chan struct{}, 1)

			hedgedStart := time.Now()
			go hedgedfunc(ctx, hedgedResolved)

			select {
			case <-resolved:
				// if the original request completes while the hedged request is
				// in transit, then don't wait for the hedged request
				duration = time.Since(start)
			case <-hedgedResolved:
				duration = time.Since(hedgedStart)
			}
		}

		q.Add(duration.Seconds(), 1)
	}
}

type hedgedDatastore struct {
	storage.OpenFGADatastore

	hedger hedgedFuncResolver
}

func NewHedgedDatastore(
	ds storage.OpenFGADatastore,
	quantile float64,
) storage.OpenFGADatastore {

	quantileApproximator := NewBoundedQuantileApproximator(1000)

	hedged := &hedgedDatastore{
		OpenFGADatastore: ds,
		hedger:           hedger(quantileApproximator, quantile),
	}

	return hedged
}

func (h *hedgedDatastore) Read(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
) (storage.TupleIterator, error) {

	var iter storage.TupleIterator
	var err error

	var once sync.Once

	h.hedger(ctx, func(ctx context.Context, resolved chan<- struct{}) {
		innerIter, innerErr := h.OpenFGADatastore.Read(ctx, store, tk)

		slowestResolver := true

		// once.Do ensures the fastest resolver will run the provided function (it only gets called once)
		once.Do(func() {
			slowestResolver = false

			iter = innerIter
			err = innerErr
			resolved <- struct{}{}
		})

		// the slowest resolver is responsible for closing the channel since it's
		// the only remaining goroutine that could potentially publish to the channel.
		if slowestResolver && innerErr == nil {
			innerIter.Stop() // stop the slowest resolvers iter to avoid leaks
			close(resolved)
		}
	})

	return iter, err
}

func (h *hedgedDatastore) ReadPage(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
	opts storage.PaginationOptions,
) ([]*openfgapb.Tuple, []byte, error) {

	var tuples []*openfgapb.Tuple
	var contToken []byte
	var err error

	var once sync.Once

	h.hedger(ctx, func(ctx context.Context, resolved chan<- struct{}) {
		innerTuples, innerContToken, innerErr := h.OpenFGADatastore.ReadPage(ctx, store, tk, opts)

		slowestResolver := true

		// once.Do ensures the fastest resolver will run the provided function (it only gets called once)
		once.Do(func() {
			slowestResolver = false

			tuples = innerTuples
			contToken = innerContToken
			err = innerErr

			resolved <- struct{}{}
		})

		// the slowest resolver is responsible for closing the channel since it's
		// the only remaining goroutine that could potentially publish to the channel.
		if slowestResolver {
			close(resolved)
		}
	})

	return tuples, contToken, err
}

func (h *hedgedDatastore) ReadUserTuple(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
) (*openfgapb.Tuple, error) {

	var tuple *openfgapb.Tuple
	var err error

	var once sync.Once

	h.hedger(ctx, func(ctx context.Context, resolved chan<- struct{}) {
		innerTuple, innerErr := h.OpenFGADatastore.ReadUserTuple(ctx, store, tk)

		slowestResolver := true

		// once.Do ensures the fastest resolver will run the provided function (it only gets called once)
		once.Do(func() {
			slowestResolver = false

			tuple = innerTuple
			err = innerErr

			resolved <- struct{}{}
		})

		// the slowest resolver is responsible for closing the channel since it's
		// the only remaining goroutine that could potentially publish to the channel.
		if slowestResolver {
			close(resolved)
		}
	})

	return tuple, err
}

func (h *hedgedDatastore) ReadUsersetTuples(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
) (storage.TupleIterator, error) {

	var iter storage.TupleIterator
	var err error

	var once sync.Once

	h.hedger(ctx, func(ctx context.Context, resolved chan<- struct{}) {
		innerIter, innerErr := h.OpenFGADatastore.ReadUsersetTuples(ctx, store, tk)

		slowestResolver := true

		// once.Do ensures the fastest resolver will run the provided function (it only gets called once)
		once.Do(func() {
			slowestResolver = false

			iter = innerIter
			err = innerErr

			resolved <- struct{}{}
		})

		// the slowest resolver is responsible for closing the channel since it's
		// the only remaining goroutine that could potentially publish to the channel.
		if slowestResolver && innerErr == nil {
			innerIter.Stop() // stop the slowest resolvers iter to avoid leaks
			close(resolved)
		}
	})

	return iter, err
}

func (h *hedgedDatastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {

	var iter storage.TupleIterator
	var err error

	var once sync.Once

	h.hedger(ctx, func(ctx context.Context, resolved chan<- struct{}) {
		innerIter, innerErr := h.OpenFGADatastore.ReadStartingWithUser(ctx, store, filter)

		slowestResolver := true

		// once.Do ensures the fastest resolver will run the provided function (it only gets called once)
		once.Do(func() {
			slowestResolver = false

			iter = innerIter
			err = innerErr

			resolved <- struct{}{}
		})

		// the slowest resolver is responsible for closing the channel since it's
		// the only remaining goroutine that could potentially publish to the channel.
		if slowestResolver && innerErr == nil {
			innerIter.Stop() // stop the slowest resolvers iter to avoid leaks
			close(resolved)
		}
	})

	return iter, err
}
