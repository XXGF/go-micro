package client

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/micro/go-micro/v2/broker"
	"github.com/micro/go-micro/v2/client/selector"
	"github.com/micro/go-micro/v2/codec"
	raw "github.com/micro/go-micro/v2/codec/bytes"
	"github.com/micro/go-micro/v2/errors"
	"github.com/micro/go-micro/v2/metadata"
	"github.com/micro/go-micro/v2/registry"
	"github.com/micro/go-micro/v2/transport"
	"github.com/micro/go-micro/v2/util/buf"
	"github.com/micro/go-micro/v2/util/pool"
)

type rpcClient struct {
	once atomic.Value
	opts Options
	pool pool.Pool
	seq  uint64
}

func newRpcClient(opt ...Option) Client {
	opts := NewOptions(opt...)

	p := pool.NewPool(
		pool.Size(opts.PoolSize),
		pool.TTL(opts.PoolTTL),
		pool.Transport(opts.Transport),
	)

	rc := &rpcClient{
		opts: opts,
		pool: p,
		seq:  0,
	}
	rc.once.Store(false)

	c := Client(rc)

	// wrap in reverse
	for i := len(opts.Wrappers); i > 0; i-- {
		c = opts.Wrappers[i-1](c)
	}

	return c
}

func (r *rpcClient) newCodec(contentType string) (codec.NewCodec, error) {
	if c, ok := r.opts.Codecs[contentType]; ok {
		return c, nil
	}
	if cf, ok := DefaultCodecs[contentType]; ok {
		return cf, nil
	}
	return nil, fmt.Errorf("Unsupported Content-Type: %s", contentType)
}

// 这里是真正的远程调用实现
func (r *rpcClient) call(ctx context.Context, node *registry.Node, req Request, resp interface{}, opts CallOptions) error {
	address := node.Address

	msg := &transport.Message{
		Header: make(map[string]string),
	}

	md, ok := metadata.FromContext(ctx)
	if ok {
		for k, v := range md {
			// don't copy Micro-Topic header, that used for pub/sub
			// this fix case then client uses the same context that received in subscriber
			if k == "Micro-Topic" {
				continue
			}
			msg.Header[k] = v
		}
	}

	// set timeout in nanoseconds
	msg.Header["Timeout"] = fmt.Sprintf("%d", opts.RequestTimeout)
	// set the content type for the request
	msg.Header["Content-Type"] = req.ContentType()
	// set the accept header
	msg.Header["Accept"] = req.ContentType()

	// setup old protocol
	cf := setupProtocol(msg, node)

	// no codec specified
	if cf == nil {
		var err error
		cf, err = r.newCodec(req.ContentType())
		if err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}
	}

	dOpts := []transport.DialOption{
		transport.WithStream(),
	}

	if opts.DialTimeout >= 0 {
		dOpts = append(dOpts, transport.WithTimeout(opts.DialTimeout))
	}

	// 这个 c 是Connection，里面封装了TransportClient，TransportClient里面封装了Socket，Socket的实现有多中，包括httpTransportSocket【http实现】 、grpcTransportSocket【rpc实现】等
	// 最终真正进行通讯的是Socket的实现
	c, err := r.pool.Get(address, dOpts...)
	if err != nil {
		return errors.InternalServerError("go.micro.client", "connection error: %v", err)
	}

	seq := atomic.LoadUint64(&r.seq)
	atomic.AddUint64(&r.seq, 1)
	codec := newRpcCodec(msg, c, cf, "")

	rsp := &rpcResponse{
		socket: c,
		codec:  codec,
	}

	stream := &rpcStream{
		id:       fmt.Sprintf("%v", seq),
		context:  ctx,
		request:  req,
		response: rsp,
		codec:    codec,
		closed:   make(chan bool),
		release:  func(err error) { r.pool.Release(c, err) },
		sendEOS:  false,
	}
	// close the stream on exiting this function
	defer stream.Close()

	// wait for error response
	ch := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- errors.InternalServerError("go.micro.client", "panic recovered: %v", r)
			}
		}()

		// send request
		// 这里才是真正发起请求
		if err := stream.Send(req.Body()); err != nil {
			ch <- err
			return
		}

		// recv request
		// 这里是接收响应信息
		if err := stream.Recv(resp); err != nil {
			ch <- err
			return
		}

		// **思考：go-micro是只能同步发送和接收，不能像grpc那样异步发送和接收？
		/*
		go func() {
		        for {
			    select {
				case <-ctx.Done():
				        log.Println("close...")
					return
				default:
					log.Println("send...")
					stream.Send(&steam.Request{
						Msg: time.Now().String(),
					})
				}
			}
		}()

		go func() {
			for {
				req, err := stream.Recv()
				if err != nil {
		                         stream.Close()
					log.Println("err:", err)
		                         cancel()
				}
				log.Println("recv:", req.Data)
			}
		}()*/

		// success
		ch <- nil
	}()

	var grr error

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		grr = errors.Timeout("go.micro.client", fmt.Sprintf("%v", ctx.Err()))
	}

	// set the stream error
	if grr != nil {
		stream.Lock()
		stream.err = grr
		stream.Unlock()

		return grr
	}

	return nil
}

func (r *rpcClient) stream(ctx context.Context, node *registry.Node, req Request, opts CallOptions) (Stream, error) {
	address := node.Address

	msg := &transport.Message{
		Header: make(map[string]string),
	}

	md, ok := metadata.FromContext(ctx)
	if ok {
		for k, v := range md {
			msg.Header[k] = v
		}
	}

	// set timeout in nanoseconds
	if opts.StreamTimeout > time.Duration(0) {
		msg.Header["Timeout"] = fmt.Sprintf("%d", opts.StreamTimeout)
	}
	// set the content type for the request
	msg.Header["Content-Type"] = req.ContentType()
	// set the accept header
	msg.Header["Accept"] = req.ContentType()

	// set old codecs
	cf := setupProtocol(msg, node)

	// no codec specified
	if cf == nil {
		var err error
		cf, err = r.newCodec(req.ContentType())
		if err != nil {
			return nil, errors.InternalServerError("go.micro.client", err.Error())
		}
	}

	dOpts := []transport.DialOption{
		transport.WithStream(),
	}

	if opts.DialTimeout >= 0 {
		dOpts = append(dOpts, transport.WithTimeout(opts.DialTimeout))
	}

	c, err := r.opts.Transport.Dial(address, dOpts...)
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", "connection error: %v", err)
	}

	// increment the sequence number
	seq := atomic.LoadUint64(&r.seq)
	atomic.AddUint64(&r.seq, 1)
	id := fmt.Sprintf("%v", seq)

	// create codec with stream id
	codec := newRpcCodec(msg, c, cf, id)

	rsp := &rpcResponse{
		socket: c,
		codec:  codec,
	}

	// set request codec
	if r, ok := req.(*rpcRequest); ok {
		r.codec = codec
	}

	stream := &rpcStream{
		id:       id,
		context:  ctx,
		request:  req,
		response: rsp,
		codec:    codec,
		// used to close the stream
		closed: make(chan bool),
		// signal the end of stream,
		sendEOS: true,
		// release func
		release: func(err error) { c.Close() },
	}

	// wait for error response
	ch := make(chan error, 1)

	go func() {
		// send the first message
		ch <- stream.Send(req.Body())
	}()

	var grr error

	select {
	case err := <-ch:
		grr = err
	case <-ctx.Done():
		grr = errors.Timeout("go.micro.client", fmt.Sprintf("%v", ctx.Err()))
	}

	if grr != nil {
		// set the error
		stream.Lock()
		stream.err = grr
		stream.Unlock()

		// close the stream
		stream.Close()
		return nil, grr
	}

	return stream, nil
}

func (r *rpcClient) Init(opts ...Option) error {
	size := r.opts.PoolSize
	ttl := r.opts.PoolTTL
	tr := r.opts.Transport

	for _, o := range opts {
		o(&r.opts)
	}

	// update pool configuration if the options changed
	if size != r.opts.PoolSize || ttl != r.opts.PoolTTL || tr != r.opts.Transport {
		// close existing pool
		r.pool.Close()
		// create new pool
		r.pool = pool.NewPool(
			pool.Size(r.opts.PoolSize),
			pool.TTL(r.opts.PoolTTL),
			pool.Transport(r.opts.Transport),
		)
	}

	return nil
}

func (r *rpcClient) Options() Options {
	return r.opts
}

// hasProxy checks if we have proxy set in the environment
func (r *rpcClient) hasProxy() bool {
	// get proxy
	if prx := os.Getenv("MICRO_PROXY"); len(prx) > 0 {
		return true
	}

	// get proxy address
	if prx := os.Getenv("MICRO_PROXY_ADDRESS"); len(prx) > 0 {
		return true
	}

	return false
}

// next returns an iterator for the next nodes to call
func (r *rpcClient) next(request Request, opts CallOptions) (selector.Next, error) {
	service := request.Service()

	// get proxy
	if prx := os.Getenv("MICRO_PROXY"); len(prx) > 0 {
		// default name
		if prx == "service" {
			prx = "go.micro.proxy"
		}
		service = prx
	}

	// get proxy address
	if prx := os.Getenv("MICRO_PROXY_ADDRESS"); len(prx) > 0 {
		opts.Address = []string{prx}
	}

	// return remote address
	if len(opts.Address) > 0 {
		nodes := make([]*registry.Node, len(opts.Address))

		for i, address := range opts.Address {
			nodes[i] = &registry.Node{
				Address: address,
				// Set the protocol
				Metadata: map[string]string{
					"protocol": "mucp",
				},
			}
		}

		// crude return method
		return func() (*registry.Node, error) {
			return nodes[time.Now().Unix()%int64(len(nodes))], nil
		}, nil
	}

	// get next nodes from the selector
	next, err := r.opts.Selector.Select(service, opts.SelectOptions...)
	if err != nil {
		if err == selector.ErrNotFound {
			return nil, errors.InternalServerError("go.micro.client", "service %s: %s", service, err.Error())
		}
		return nil, errors.InternalServerError("go.micro.client", "error selecting %s node: %s", service, err.Error())
	}

	return next, nil
}

// 这里的Call方法，是在*.proto协议文件生成的 *.micro.go文件里被调用的，例如：
/*
func (c *cmemberService) ListByCid(ctx context.Context, in *ListByCidReq, opts ...client.CallOption) (*ListByCidResp, error) {
	// 通过 NewRequest 初始化了请求实例，包含服务名称及请求端点、参数信息
	req := c.c.NewRequest(c.name, "Cmember.ListByCid", in)

    // 初始化了响应实例
	out := new(ListByCidResp)

    // 下来是调用 Client 的 Call 函数，所有请求调用处理的核心逻辑（服务发现、节点选择、请求处理、超时、重试、编码）都在这个函数里
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
*/
func (r *rpcClient) Call(ctx context.Context, request Request, response interface{}, opts ...CallOption) error {
	// make a copy of call opts
	callOpts := r.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	// 1.首先会通过 rpcClient 类的 next 方法获取远程服务节点，在未设置系统环境变量 MICRO_PROXY_ADDRESS 的情况下会执行 Selector 的 Select 方法获取服务节点
	//   默认的 Selector 初始化操作位于 ~/go/hello/src/github.com/micro/go-micro/client/options.go 的 newOptions 方法（该方法会在 client.go 的初始化操作中调用）
	//   Selector 依赖于 Registry 组件，如果没有额外设置的话，基于系统默认的 Registry 实现
	//   Selector 默认的负载均衡策略使用的是随机算法，并且会在本地对节点选择结果进行缓存。

	//这里调用 rpcClient 的 next 函数返回的并不是真正的节点而是一个匿名函数
	next, err := r.next(request, callOpts)
	if err != nil {
		return err
	}

	// check if we already have a deadline
	d, ok := ctx.Deadline()
	if !ok {
		// no deadline so we create a new one
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, callOpts.RequestTimeout)
		defer cancel()
	} else {
		// got a deadline so no need to setup context
		// but we need to set the timeout we pass along
		opt := WithRequestTimeout(d.Sub(time.Now()))
		opt(&callOpts)
	}

	// should we noop right here?
	select {
	case <-ctx.Done():
		return errors.Timeout("go.micro.client", fmt.Sprintf("%v", ctx.Err()))
	default:
	}

	// make copy of call method
	rcall := r.call

	// wrap the call in reverse
	for i := len(callOpts.CallWrappers); i > 0; i-- {
		rcall = callOpts.CallWrappers[i-1](rcall)
	}

	// return errors.New("go.micro.client", "request timeout", 408)
	call := func(i int) error {
		// call backoff first. Someone may want an initial start delay
		t, err := callOpts.Backoff(ctx, request, i)
		if err != nil {
			return errors.InternalServerError("go.micro.client", "backoff error: %v", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		// select next node
		// 1.这里是调用上面返回的匿名函数，取得真正的服务节点信息
		node, err := next()
		service := request.Service()
		if err != nil {
			if err == selector.ErrNotFound {
				return errors.InternalServerError("go.micro.client", "service %s: %s", service, err.Error())
			}
			return errors.InternalServerError("go.micro.client", "error getting next %s node: %s", service, err.Error())
		}

		// make the call
		// 2.如果返回节点成功则调用 rcall 函数（即 rpcClient 的 call 函数）发起远程网络请求（通过协程实现）
		err = rcall(ctx, node, request, response, callOpts)
		// 3.然后对服务调用成功与否通过 Selector 的 Mark 函数进行标记（以便后续对服务进行监控和治理）
		r.opts.Selector.Mark(service, node, err)
		// 4.最后在 Call 函数中，也是通过协程发起对上述 call 匿名函数的调用
		return err
	}

	// get the retries
	retries := callOpts.Retries

	// disable retries when using a proxy
	if r.hasProxy() {
		retries = 0
	}

	ch := make(chan error, retries+1)
	var gerr error

	for i := 0; i <= retries; i++ {
		go func(i int) {
			// 真正进行调用
			ch <- call(i)
		}(i)

		select {
		case <-ctx.Done():
			return errors.Timeout("go.micro.client", fmt.Sprintf("call timeout: %v", ctx.Err()))
		case err := <-ch:
			// if the call succeeded lets bail early
			if err == nil {
				return nil
			}

			retry, rerr := callOpts.Retry(ctx, request, i, err)
			if rerr != nil {
				return rerr
			}

			if !retry {
				return err
			}

			gerr = err
		}
	}

	return gerr
}

func (r *rpcClient) Stream(ctx context.Context, request Request, opts ...CallOption) (Stream, error) {
	// make a copy of call opts
	callOpts := r.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	next, err := r.next(request, callOpts)
	if err != nil {
		return nil, err
	}

	// should we noop right here?
	select {
	case <-ctx.Done():
		return nil, errors.Timeout("go.micro.client", fmt.Sprintf("%v", ctx.Err()))
	default:
	}

	call := func(i int) (Stream, error) {
		// call backoff first. Someone may want an initial start delay
		t, err := callOpts.Backoff(ctx, request, i)
		if err != nil {
			return nil, errors.InternalServerError("go.micro.client", "backoff error: %v", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		node, err := next()
		service := request.Service()
		if err != nil {
			if err == selector.ErrNotFound {
				return nil, errors.InternalServerError("go.micro.client", "service %s: %s", service, err.Error())
			}
			return nil, errors.InternalServerError("go.micro.client", "error getting next %s node: %s", service, err.Error())
		}

		stream, err := r.stream(ctx, node, request, callOpts)
		r.opts.Selector.Mark(service, node, err)
		return stream, err
	}

	type response struct {
		stream Stream
		err    error
	}

	// get the retries
	retries := callOpts.Retries

	// disable retries when using a proxy
	if r.hasProxy() {
		retries = 0
	}

	ch := make(chan response, retries+1)
	var grr error

	for i := 0; i <= retries; i++ {
		go func(i int) {
			s, err := call(i)
			ch <- response{s, err}
		}(i)

		select {
		case <-ctx.Done():
			return nil, errors.Timeout("go.micro.client", fmt.Sprintf("call timeout: %v", ctx.Err()))
		case rsp := <-ch:
			// if the call succeeded lets bail early
			if rsp.err == nil {
				return rsp.stream, nil
			}

			retry, rerr := callOpts.Retry(ctx, request, i, rsp.err)
			if rerr != nil {
				return nil, rerr
			}

			if !retry {
				return nil, rsp.err
			}

			grr = rsp.err
		}
	}

	return nil, grr
}

func (r *rpcClient) Publish(ctx context.Context, msg Message, opts ...PublishOption) error {
	options := PublishOptions{
		Context: context.Background(),
	}
	for _, o := range opts {
		o(&options)
	}

	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}

	id := uuid.New().String()
	md["Content-Type"] = msg.ContentType()
	md["Micro-Topic"] = msg.Topic()
	md["Micro-Id"] = id

	// set the topic
	topic := msg.Topic()

	// get the exchange
	if len(options.Exchange) > 0 {
		topic = options.Exchange
	}

	// encode message body
	cf, err := r.newCodec(msg.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}

	var body []byte

	// passed in raw data
	if d, ok := msg.Payload().(*raw.Frame); ok {
		body = d.Data
	} else {
		// new buffer
		b := buf.New(nil)

		if err := cf(b).Write(&codec.Message{
			Target: topic,
			Type:   codec.Event,
			Header: map[string]string{
				"Micro-Id":    id,
				"Micro-Topic": msg.Topic(),
			},
		}, msg.Payload()); err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}

		// set the body
		body = b.Bytes()
	}

	if !r.once.Load().(bool) {
		if err = r.opts.Broker.Connect(); err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}
		r.once.Store(true)
	}

	return r.opts.Broker.Publish(topic, &broker.Message{
		Header: md,
		Body:   body,
	})
}

func (r *rpcClient) NewMessage(topic string, message interface{}, opts ...MessageOption) Message {
	return newMessage(topic, message, r.opts.ContentType, opts...)
}

func (r *rpcClient) NewRequest(service, method string, request interface{}, reqOpts ...RequestOption) Request {
	return newRequest(service, method, request, r.opts.ContentType, reqOpts...)
}

func (r *rpcClient) String() string {
	return "mucp"
}
