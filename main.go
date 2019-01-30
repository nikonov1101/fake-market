package main

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sonm-io/core/insonmnia/logging"
	"github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
)

// TODO(sshaman1101):
//  1. add gGRC+TLS;
//  2. REST;
//  3. command interface to create ANY entity quickly,
//     probably REST API + simple web-page.

func main() {
	market := newFakeMarket()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = market.run(ctx)
}

type inmemOrderStorage struct {
	mu     sync.Mutex
	orders map[uint64]*sonm.Order
	lastID uint64
}

func newInmemStorage() *inmemOrderStorage {
	return &inmemOrderStorage{
		orders: map[uint64]*sonm.Order{},
		lastID: 0,
	}
}

func (s *inmemOrderStorage) getNextID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastID++
	return s.lastID
}

func (s *inmemOrderStorage) all() []*sonm.Order {
	orders := make([]*sonm.Order, 0, len(s.orders))

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ord := range s.orders {
		orders = append(orders, ord)
	}

	return orders
}

func (s *inmemOrderStorage) byID(id uint64) (*sonm.Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ord, ok := s.orders[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	return ord, nil
}

func (s *inmemOrderStorage) add(order *sonm.Order) (*sonm.Order, error) {
	nid := s.getNextID()
	order.Id = sonm.NewBigInt(big.NewInt(int64(nid)))

	s.mu.Lock()
	defer s.mu.Unlock()

	s.orders[nid] = order
	return order, nil
}

func (s *inmemOrderStorage) delete(id uint64, caller common.Address) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ord, ok := s.orders[id]
	if !ok {
		return fmt.Errorf("not found")
	}

	if ord.AuthorID.Unwrap().Hex() != caller.Hex() {
		return fmt.Errorf("permission denied")
	}

	delete(s.orders, id)
	return nil
}

type fakeMarket struct {
	sonm.MarketServer
	log     *zap.Logger
	storage *inmemOrderStorage
}

func newFakeMarket() *fakeMarket {
	log, err := logging.BuildLogger(logging.Config{
		Level:  logging.NewLevel(zapcore.DebugLevel),
		Output: "stdout",
	})
	if err != nil {
		panic(err)
	}

	return &fakeMarket{
		log:     log,
		storage: newInmemStorage(),
	}
}

func (m *fakeMarket) run(ctx context.Context) error {
	xx := xgrpc.NewServer(m.log)
	sonm.RegisterMarketServer(xx, m)

	wg, ctx := errgroup.WithContext(ctx)

	lc := net.ListenConfig{}
	ls, err := lc.Listen(ctx, "tcp", "0.0.0.0:8989")
	if err != nil {
		return fmt.Errorf("failed to create listener: %v", err)
	}

	wg.Go(func() error {
		m.log.Info("starting fake market", zap.String("addr", ls.Addr().String()))
		return xx.Serve(ls)
	})

	<-ctx.Done()
	return wg.Wait()
}

func (m *fakeMarket) GetOrders(ctx context.Context, _ *sonm.Count) (*sonm.GetOrdersReply, error) {
	return &sonm.GetOrdersReply{Orders: m.storage.all()}, nil
}

func (m *fakeMarket) CreateOrder(ctx context.Context, bid *sonm.BidOrder) (*sonm.Order, error) {
	from := m.getCallerAddr(ctx)
	if from == nil {
		return nil, fmt.Errorf("bad request: no author")
	}

	allowed := map[string]int{
		"cpu-sysbench-multi":  0,
		"cpu-sysbench-single": 1,
		"cpu-cores":           2,
		"ram-size":            3,
		"storage-size":        4,
		"net-download":        5,
		"net-upload":          6,
		"gpu-count":           7,
		"gpu-mem":             8,
		"gpu-eth-hashrate":    9,
		"gpu-cash-hashrate":   10,
		"gpu-redshift":        11,
		"cpu-cryptonight":     12,
	}

	bx := &sonm.Benchmarks{Values: make([]uint64, len(allowed))}
	for k, v := range bid.GetResources().GetBenchmarks() {
		idx := allowed[k]
		bx.Values[idx] = v
	}

	nf := &sonm.NetFlags{}
	nf.SetIncoming(bid.GetResources().GetNetwork().GetIncoming())
	nf.SetOutbound(bid.GetResources().GetNetwork().GetOutbound())
	nf.SetOverlay(bid.GetResources().GetNetwork().GetOverlay())

	ord := &sonm.Order{
		OrderType:      sonm.OrderType_BID,
		OrderStatus:    sonm.OrderStatus_ORDER_ACTIVE,
		AuthorID:       sonm.NewEthAddress(*from),
		CounterpartyID: bid.Counterparty,
		Duration:       uint64(bid.GetDuration().Unwrap().Seconds()),
		Price:          bid.GetPrice().GetPerSecond(),
		IdentityLevel:  bid.GetIdentity(),
		Blacklist:      bid.GetBlacklist().Unwrap().Hex(),
		Tag:            []byte(bid.GetTag()),
		Netflags:       nf,
		Benchmarks:     bx,
	}

	return m.storage.add(ord)
}

func (m *fakeMarket) GetOrderByID(_ context.Context, id *sonm.ID) (*sonm.Order, error) {
	bigID, ok := big.NewInt(0).SetString(id.GetId(), 10)
	if !ok {
		return nil, fmt.Errorf("failed to convert id to numeric")
	}

	return m.storage.byID(bigID.Uint64())
}

func (m *fakeMarket) CancelOrder(ctx context.Context, id *sonm.ID) (*sonm.Empty, error) {
	bigID, ok := big.NewInt(0).SetString(id.GetId(), 10)
	if !ok {
		return nil, fmt.Errorf("failed to convert id to numeric")
	}

	from := m.getCallerAddr(ctx)
	if from == nil {
		return nil, fmt.Errorf("bad request: no author")
	}

	if err := m.storage.delete(bigID.Uint64(), *from); err != nil {
		return nil, err
	}

	return &sonm.Empty{}, nil
}

func (m *fakeMarket) CancelOrders(ctx context.Context, ids *sonm.OrderIDs) (*sonm.ErrorByID, error) {
	from := m.getCallerAddr(ctx)
	if from == nil {
		return nil, fmt.Errorf("bad request: no author")
	}

	resp := &sonm.ErrorByID{Response: []*sonm.ErrorByID_Item{}}
	for _, id := range ids.GetIds() {
		item := &sonm.ErrorByID_Item{Id: id}
		if err := m.storage.delete(id.Unwrap().Uint64(), *from); err != nil {
			item.Error = err.Error()
		}

		resp.Response = append(resp.Response, item)
	}

	return resp, nil
}

func (m *fakeMarket) Purge(ctx context.Context, _ *sonm.Empty) (*sonm.Empty, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *fakeMarket) PurgeVerbose(context.Context, *sonm.Empty) (*sonm.ErrorByID, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *fakeMarket) getCallerAddr(ctx context.Context) *common.Address {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		m.log.Warn("context have no meta")
		return &common.Address{}
	}

	from := md.Get("from")
	if len(from) == 0 {
		m.log.Warn("context have key, but values are empty")
		return &common.Address{}
	}

	a := common.HexToAddress(from[0])
	return &a
}
