package fanout

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/mattbostock/timbala/internal/cluster"
	"github.com/mattbostock/timbala/internal/read"
	"github.com/mattbostock/timbala/internal/write"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context/ctxhttp"
)

const readTimeoutSeconds = 30 * time.Second

var httpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			DualStack: true,
			KeepAlive: 10 * time.Minute,
			Timeout:   2 * time.Second,
		}).DialContext,
		ExpectContinueTimeout: 5 * time.Second,
		IdleConnTimeout:       10 * time.Minute,
		ResponseHeaderTimeout: 5 * time.Second,
	}}

type fanoutStorage struct {
	clstr      cluster.Cluster
	localStore storage.Storage
	log        *logrus.Logger
}

func New(c cluster.Cluster, l *logrus.Logger, s storage.Storage) *fanoutStorage {
	return &fanoutStorage{
		clstr:      c,
		localStore: s,
		log:        l,
	}
}

func (f *fanoutStorage) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	clients, err := f.remoteClients()
	if err != nil {
		return nil, err
	}

	localQuerier, err := f.localStore.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	queriers := append([]storage.Querier{}, localQuerier)

	for _, c := range clients {
		queriers = append(queriers, fanoutQuerier{
			ctx:     ctx,
			client:  c,
			maxt:    maxt,
			mint:    mint,
			storage: f,
		})
	}

	return storage.NewMergeQuerier(queriers), nil
}

func (f *fanoutStorage) Appender() (storage.Appender, error) {
	clients, err := f.remoteClients()
	if err != nil {
		return nil, err
	}

	localAppender, err := f.localStore.Appender()
	if err != nil {
		return nil, err
	}

	var appenders []storage.Appender
	appenders = append(appenders, localAppender)

	for _, c := range clients {
		appenders = append(appenders, &mergeAppender{
			//ctx:     ctx, FIXME
			client:  c,
			storage: f,
		})
	}

	return newMergeAppender(appenders), nil
}

func (f *fanoutStorage) StartTime() (int64, error) {
	return 0, nil
}

func (f *fanoutStorage) Close() error {
	return nil
}

func (f *fanoutStorage) remoteClients() ([]*remoteClient, error) {
	// FIXME handle cluster size changes
	clients := make([]*remoteClient, 0, len(f.clstr.Nodes()))

	for _, n := range f.clstr.Nodes() {
		if n.Name() == f.clstr.LocalNode().Name() {
			continue
		}

		addr, err := n.HTTPAddr()
		if err != nil {
			return nil, err
		}
		clients = append(clients, &remoteClient{
			httpURL: "http://" + addr,
		})
	}

	return clients, nil
}

type fanoutQuerier struct {
	ctx        context.Context
	client     *remoteClient
	mint, maxt int64
	storage    *fanoutStorage
}

func (q fanoutQuerier) Select(matchers ...*labels.Matcher) storage.SeriesSet {
	protoMatchers, err := toLabelMatchers(matchers)
	if err != nil {
		return errSeriesSet{err}
	}

	res, err := q.client.Read(q.ctx, q.mint, q.maxt, protoMatchers)
	// FIXME: Don't fail if just one node fails to respond
	if err != nil {
		return errSeriesSet{err}
	}

	series := make([]storage.Series, 0, len(res))
	for _, ts := range res {
		labels := labelPairsToLabels(ts.Labels)
		series = append(series, &concreteSeries{
			labels:  labels,
			samples: ts.Samples,
		})
	}
	sort.Sort(byLabel(series))
	return &concreteSeriesSet{
		series: series,
	}
}

func (_ fanoutQuerier) LabelValues(name string) ([]string, error) {
	panic("not implemented")
}

func (_ fanoutQuerier) Close() error {
	// Nothing to do
	return nil
}

func newMergeAppender(c context.Context, cl *remoteClient, s *fanoutStorage, ts []*prompb.TimeSeries) *mergeAppender {
	// FIXME handle change in cluster size
	seriesToNodes := make(seriesNodeMap, len(wr.clstr.Nodes()))
	for _, n := range wr.clstr.Nodes() {
		seriesToNodes[*n] = make(seriesMap, numPreallocTimeseries)
	}

	pSalt := []byte(r.Header.Get(HTTPHeaderPartitionKeySalt))
	for _, ts := range req.Timeseries {
		m := make(labels.Labels, 0, len(ts.Labels))
		for _, l := range ts.Labels {
			m = append(m, labels.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		sort.Stable(m)
		// FIXME: Handle collisions
		mHash := m.Hash()

		for _, s := range ts.Samples {
			timestamp := time.Unix(s.Timestamp/1000, (s.Timestamp-s.Timestamp/1000)*1e6)
			// FIXME: Avoid panic if the cluster is not yet initialised
			pKey := cluster.PartitionKey(pSalt, timestamp, mHash)
			for _, n := range wr.clstr.NodesByPartitionKey(pKey) {
				if _, ok := seriesToNodes[*n][mHash]; !ok {
					// FIXME handle change in cluster size
					seriesToNodes[*n][mHash] = &prompb.TimeSeries{
						Labels:  ts.Labels,
						Samples: make([]*prompb.Sample, 0, len(ts.Samples)),
					}
				}
				seriesToNodes[*n][mHash].Samples = append(seriesToNodes[*n][mHash].Samples, s)
			}
		}
		// FIXME: sort samples by time?
	}

	return &mergeAppender{
		ctx:     c,
		client:  cl,
		storage: s,
		series:  ts,
	}
}

type mergeAppender struct {
	sync.Mutex

	ctx     context.Context
	client  *remoteClient
	storage *fanoutStorage
	series  []*prompb.TimeSeries
}

func (a *mergeAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	a.Lock()
	defer a.Unlock()

	err := a.AddFast(l, ref, t, v)
	if err != nil {
		return 0, nil
	}

	return ref, nil
}

func (a *mergeAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	a.Lock()
	defer a.Unlock()

	panic("not implemented")
}

func (a *mergeAppender) Commit() error {
	a.Lock()
	defer a.Unlock()

	// send request
	panic("not implemented")
}

func (a *mergeAppender) Rollback() error {
	a.Lock()
	defer a.Unlock()

	// wipe data
	panic("not implemented")
}

type remoteClient struct{ httpURL string }

func (c *remoteClient) Read(ctx context.Context, from, through int64, matchers []*prompb.LabelMatcher) ([]*prompb.TimeSeries, error) {
	req := &prompb.ReadRequest{
		// FIXME: Support batching multiple queries into one read
		// request, as the protobuf interface allows for it.
		Queries: []*prompb.Query{{
			StartTimestampMs: from,
			EndTimestampMs:   through,
			Matchers:         matchers,
		}},
	}

	data, err := req.Marshal()
	if err != nil {
		return nil, fmt.Errorf("unable to marshal read request: %v", err)
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.httpURL+read.Route, bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %v", err)
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set(read.HTTPHeaderRemoteRead, read.HTTPHeaderRemoteReadVersion)
	httpReq.Header.Set(read.HTTPHeaderInternalRead, read.HTTPHeaderInternalReadVersion)

	ctx, cancel := context.WithTimeout(ctx, readTimeoutSeconds)
	defer cancel()

	httpResp, err := ctxhttp.Do(ctx, httpClient, httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}

	compressed, err = ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	var resp prompb.ReadResponse
	err = resp.Unmarshal(uncompressed)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %v", err)
	}

	if len(resp.Results) != len(req.Queries) {
		return nil, fmt.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
	}

	return resp.Results[0].Timeseries, nil
}

func (c *remoteClient) Write(ctx context.Context, series []*prompb.TimeSeries) error {
	req := &prompb.WriteRequest{
		Timeseries: series,
	}

	data, err := req.Marshal()
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	nodeReq, err := http.NewRequest("POST", c.httpURL+write.Route, bytes.NewBuffer(compressed))
	if err != nil {
		return err
	}
	nodeReq.Header.Add("Content-Encoding", "snappy")
	nodeReq.Header.Set("Content-Type", "application/x-protobuf")
	nodeReq.Header.Set(write.HTTPHeaderRemoteWrite, write.HTTPHeaderRemoteWriteVersion)
	nodeReq.Header.Set(write.HTTPHeaderInternalWrite, write.HTTPHeaderInternalWriteVersion)

	httpResp, err := ctxhttp.Do(ctx, http.DefaultClient, nodeReq)
	if err != nil {
		return nil
	}

	httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("got HTTP %d status code", httpResp.StatusCode)
	}

	return nil
}

func labelPairsToLabels(labelPairs []*prompb.Label) labels.Labels {
	result := make(labels.Labels, 0, len(labelPairs))
	for _, l := range labelPairs {
		result = append(result, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Sort(result)
	return result
}

// errSeriesSet implements storage.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() storage.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []storage.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

// concreteSeries implementes storage.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []*prompb.Sample
}

func (c *concreteSeries) Labels() labels.Labels {
	return c.labels
}

func (c *concreteSeries) Iterator() storage.SeriesIterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements storage.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) storage.SeriesIterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= t
	})
	return c.cur < len(c.series.samples)
}

func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return s.Timestamp, s.Value
}

func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

func (c *concreteSeriesIterator) Err() error {
	return nil
}

type byLabel []storage.Series

func (a byLabel) Len() int           { return len(a) }
func (a byLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLabel) Less(i, j int) bool { return labels.Compare(a[i].Labels(), a[j].Labels()) < 0 }

// FIXME: Deduplicate this code copied from the read package (copied from Prometheus)
func toLabelMatchers(matchers []*labels.Matcher) ([]*prompb.LabelMatcher, error) {
	result := make([]*prompb.LabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mType prompb.LabelMatcher_Type
		switch matcher.Type {
		case labels.MatchEqual:
			mType = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			mType = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			mType = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			mType = prompb.LabelMatcher_NRE
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		result = append(result, &prompb.LabelMatcher{
			Type:  mType,
			Name:  string(matcher.Name),
			Value: string(matcher.Value),
		})
	}
	return result, nil
}

type seriesNodeMap map[cluster.Node]seriesMap
type seriesMap map[uint64]*prompb.TimeSeries
