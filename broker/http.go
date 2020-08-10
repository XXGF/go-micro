// Package http provides a http based message broker
package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/micro/go-micro/v2/codec/json"
	merr "github.com/micro/go-micro/v2/errors"
	"github.com/micro/go-micro/v2/registry"
	"github.com/micro/go-micro/v2/registry/cache"
	maddr "github.com/micro/go-micro/v2/util/addr"
	mnet "github.com/micro/go-micro/v2/util/net"
	mls "github.com/micro/go-micro/v2/util/tls"
	"golang.org/x/net/http2"
)

// HTTP Broker is a point to point async broker
type httpBroker struct {
	id      string
	// IP和端口号
	address string
	opts    Options

	mux *http.ServeMux

	// 发Http Post请求，进行消息的Publish
	c *http.Client
	r registry.Registry

	sync.RWMutex
	// httpSubscriber里封装了topic和对订阅的消息进行处理的函数
	// key是topic
	subscribers map[string][]*httpSubscriber
	running     bool
	exit        chan chan error

	// offline message inbox
	mtx   sync.RWMutex
	// 存放需要进行Publish的消息
	// key是topic
	inbox map[string][][]byte
}

type httpSubscriber struct {
	opts  SubscribeOptions
	id    string
	topic string
	fn    Handler
	svc   *registry.Service
	hb    *httpBroker
}

type httpEvent struct {
	m   *Message
	t   string
	err error
}

var (
	DefaultPath      = "/"
	DefaultAddress   = "127.0.0.1:0"
	serviceName      = "micro.http.broker"
	broadcastVersion = "ff.http.broadcast"
	registerTTL      = time.Minute
	registerInterval = time.Second * 30
)

func init() {
	rand.Seed(time.Now().Unix())
}

func newTransport(config *tls.Config) *http.Transport {
	if config == nil {
		config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	dialTLS := func(network string, addr string) (net.Conn, error) {
		return tls.Dial(network, addr, config)
	}

	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		DialTLS:             dialTLS,
	}
	runtime.SetFinalizer(&t, func(tr **http.Transport) {
		(*tr).CloseIdleConnections()
	})

	// setup http2
	http2.ConfigureTransport(t)

	return t
}

// 这里返回的 httpBroker 就是实现了 Broker 接口的 HTTP Broker 实现类
func newHttpBroker(opts ...Option) Broker {
	options := Options{
		Codec:    json.Marshaler{},
		Context:  context.TODO(),
		Registry: registry.DefaultRegistry,
	}

	for _, o := range opts {
		o(&options)
	}

	// set address
	addr := DefaultAddress

	if len(options.Addrs) > 0 && len(options.Addrs[0]) > 0 {
		addr = options.Addrs[0]
	}

	h := &httpBroker{
		id:          uuid.New().String(),
		address:     addr,
		opts:        options,
		r:           options.Registry,
		c:           &http.Client{Transport: newTransport(options.TLSConfig)},
		subscribers: make(map[string][]*httpSubscriber),
		exit:        make(chan chan error),
		mux:         http.NewServeMux(),
		inbox:       make(map[string][][]byte),
	}

	// specify the message handler
	h.mux.Handle(DefaultPath, h)

	// get optional handlers
	if h.opts.Context != nil {
		handlers, ok := h.opts.Context.Value("http_handlers").(map[string]http.Handler)
		if ok {
			for pattern, handler := range handlers {
				h.mux.Handle(pattern, handler)
			}
		}
	}

	return h
}

func (h *httpEvent) Ack() error {
	return nil
}

func (h *httpEvent) Error() error {
	return h.err
}

func (h *httpEvent) Message() *Message {
	return h.m
}

func (h *httpEvent) Topic() string {
	return h.t
}

func (h *httpSubscriber) Options() SubscribeOptions {
	return h.opts
}

func (h *httpSubscriber) Topic() string {
	return h.topic
}

func (h *httpSubscriber) Unsubscribe() error {
	return h.hb.unsubscribe(h)
}

func (h *httpBroker) saveMessage(topic string, msg []byte) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// get messages
	c := h.inbox[topic]

	// save message
	c = append(c, msg)

	// max length 64
	if len(c) > 64 {
		c = c[:64]
	}

	// save inbox
	h.inbox[topic] = c
}

func (h *httpBroker) getMessage(topic string, num int) [][]byte {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// get messages
	c, ok := h.inbox[topic]
	if !ok {
		return nil
	}

	// more message than requests
	if len(c) >= num {
		msg := c[:num]
		h.inbox[topic] = c[num:]
		return msg
	}

	// reset inbox
	h.inbox[topic] = nil

	// return all messages
	return c
}

func (h *httpBroker) subscribe(s *httpSubscriber) error {
	h.Lock()
	defer h.Unlock()

	if err := h.r.Register(s.svc, registry.RegisterTTL(registerTTL)); err != nil {
		return err
	}

	h.subscribers[s.topic] = append(h.subscribers[s.topic], s)
	return nil
}

func (h *httpBroker) unsubscribe(s *httpSubscriber) error {
	h.Lock()
	defer h.Unlock()

	//nolint:prealloc
	var subscribers []*httpSubscriber

	// look for subscriber
	for _, sub := range h.subscribers[s.topic] {
		// deregister and skip forward
		if sub == s {
			_ = h.r.Deregister(sub.svc)
			continue
		}
		// keep subscriber
		subscribers = append(subscribers, sub)
	}

	// set subscribers
	h.subscribers[s.topic] = subscribers

	return nil
}

func (h *httpBroker) run(l net.Listener) {
	t := time.NewTicker(registerInterval)
	defer t.Stop()

	for {
		select {
		// heartbeat for each subscriber
		case <-t.C:pub
			h.RLock()
			for _, subs := range h.subscribers {
				for _, sub := range subs {
					_ = h.r.Register(sub.svc, registry.RegisterTTL(registerTTL))
				}
			}
			h.RUnlock()
		// received exit signal
		case ch := <-h.exit:
			ch <- l.Close()
			h.RLock()
			for _, subs := range h.subscribers {
				for _, sub := range subs {
					_ = h.r.Deregister(sub.svc)
				}
			}
			h.RUnlock()
			return
		}
	}
}

// 处理订阅的Topic推送消息的HTTP请求
// 该方法只接受 POST 请求，
// 对于读取到的异步消息会通过 httpBroker 实现的解码器进行解码后，
// 从 h.subscribers[topic] 中获取所有的 httpSubscriber 实例，
// 并调用其中的 fn 属性指向的处理器方法对消息进行处理
func (h *httpBroker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		err := merr.BadRequest("go.micro.broker", "Method not allowed")
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
		return
	}
	defer req.Body.Close()

	req.ParseForm()

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		errr := merr.InternalServerError("go.micro.broker", "Error reading request body: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	// 消息解码
	var m *Message
	if err = h.opts.Codec.Unmarshal(b, &m); err != nil {
		errr := merr.InternalServerError("go.micro.broker", "Error parsing request body: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	// 取出主题
	topic := m.Header["Micro-Topic"]
	//delete(m.Header, ":topic")

	if len(topic) == 0 {
		errr := merr.InternalServerError("go.micro.broker", "Topic not found")
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	p := &httpEvent{m: m, t: topic}
	id := req.Form.Get("id")

	//nolint:prealloc
	var subs []Handler

	h.RLock()
	// 取出订阅消息处理函数，这个进行处理
	for _, subscriber := range h.subscribers[topic] {
		if id != subscriber.id {
			continue
		}
		subs = append(subs, subscriber.fn)
	}
	h.RUnlock()

	// execute the handler
	// 取出订阅消息处理函数，这个进行处理
	for _, fn := range subs {
		p.err = fn(p)
	}
}

func (h *httpBroker) Address() string {
	h.RLock()
	defer h.RUnlock()
	return h.address
}

func (h *httpBroker) Connect() error {
	h.RLock()
	if h.running {
		h.RUnlock()
		return nil
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	var l net.Listener
	var err error

	if h.opts.Secure || h.opts.TLSConfig != nil {
		config := h.opts.TLSConfig

		fn := func(addr string) (net.Listener, error) {
			if config == nil {
				hosts := []string{addr}

				// check if its a valid host:port
				if host, _, err := net.SplitHostPort(addr); err == nil {
					if len(host) == 0 {
						hosts = maddr.IPs()
					} else {
						hosts = []string{host}
					}
				}

				// generate a certificate
				cert, err := mls.Certificate(hosts...)
				if err != nil {
					return nil, err
				}
				config = &tls.Config{Certificates: []tls.Certificate{cert}}
			}
			return tls.Listen("tcp", addr, config)
		}

		l, err = mnet.Listen(h.address, fn)
	} else {
		fn := func(addr string) (net.Listener, error) {
			return net.Listen("tcp", addr)
		}

		l, err = mnet.Listen(h.address, fn)
	}

	if err != nil {
		return err
	}

	addr := h.address
	h.address = l.Addr().String()

	go http.Serve(l, h.mux)
	go func() {
		h.run(l)
		h.Lock()
		h.opts.Addrs = []string{addr}
		h.address = addr
		h.Unlock()
	}()

	// get registry
	reg := h.opts.Registry
	if reg == nil {
		reg = registry.DefaultRegistry
	}
	// set cache
	h.r = cache.New(reg)

	// set running
	h.running = true
	return nil
}

func (h *httpBroker) Disconnect() error {
	h.RLock()
	if !h.running {
		h.RUnlock()
		return nil
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	// stop cache
	rc, ok := h.r.(cache.Cache)
	if ok {
		rc.Stop()
	}

	// exit and return err
	ch := make(chan error)
	h.exit <- ch
	err := <-ch

	// set not running
	h.running = false
	return err
}

func (h *httpBroker) Init(opts ...Option) error {
	h.RLock()
	if h.running {
		h.RUnlock()
		return errors.New("cannot init while connected")
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	for _, o := range opts {
		o(&h.opts)
	}

	if len(h.opts.Addrs) > 0 && len(h.opts.Addrs[0]) > 0 {
		h.address = h.opts.Addrs[0]
	}

	if len(h.id) == 0 {
		h.id = "go.micro.http.broker-" + uuid.New().String()
	}

	// get registry
	reg := h.opts.Registry
	if reg == nil {
		reg = registry.DefaultRegistry
	}

	// get cache
	if rc, ok := h.r.(cache.Cache); ok {
		rc.Stop()
	}

	// set registry
	h.r = cache.New(reg)

	// reconfigure tls config
	if c := h.opts.TLSConfig; c != nil {
		h.c = &http.Client{
			Transport: newTransport(c),
		}
	}

	return nil
}

func (h *httpBroker) Options() Options {
	return h.opts
}

func (h *httpBroker) Publish(topic string, msg *Message, opts ...PublishOption) error {
	// create the message first
	m := &Message{
		Header: make(map[string]string),
		Body:   msg.Body,
	}

	for k, v := range msg.Header {
		m.Header[k] = v
	}

	m.Header["Micro-Topic"] = topic

	// encode the message
	b, err := h.opts.Codec.Marshal(m)
	if err != nil {
		return err
	}

	// save the message
	// 1.对消息进行编码并保存到 httpBroker 的 inbox 中，
	// inbox 是一个字典结构，以 topic 作为键，以消息数组作为值，
	// 新增的消息都会追加到这个数组上。
	h.saveMessage(topic, b)

	// now attempt to get the service
	h.RLock()
	// 2.从Registry获取部署topic服务的节点信息，
	// 我们前面提到 h.r 是通过 cache.New 返回的封装了默认 Registry 实现的 cache 类实例，
	// 这里的 h.r 类似我们前介绍 Selector 组件时提到的 registrySelector.rc 属性，
	// 对应的 GetService 方法定义在 src/github.com/micro/go-micro/registry/cache/rcache.go 中
	s, err := h.r.GetService(serviceName)
	if err != nil {
		h.RUnlock()
		return err
	}
	h.RUnlock()

	pub := func(node *registry.Node, t string, b []byte) error {
		scheme := "http"

		// check if secure is added in metadata
		if node.Metadata["secure"] == "true" {
			scheme = "https"
		}

		vals := url.Values{}
		vals.Add("id", node.Id)

		// 请求uri
		uri := fmt.Sprintf("%s://%s%s?%s", scheme, node.Address, DefaultPath, vals.Encode())
		// 发post请求
		r, err := h.c.Post(uri, "application/json", bytes.NewReader(b))
		if err != nil {
			return err
		}

		// discard response body
		io.Copy(ioutil.Discard, r.Body)
		r.Body.Close()
		return nil
	}

	srv := func(s []*registry.Service, b []byte) {
		for _, service := range s {
			var nodes []*registry.Node

			for _, node := range service.Nodes {
				// only use nodes tagged with broker http
				if node.Metadata["broker"] != "http" {
					continue
				}

				// look for nodes for the topic
				if node.Metadata["topic"] != topic {
					continue
				}

				nodes = append(nodes, node)
			}

			// only process if we have nodes
			if len(nodes) == 0 {
				continue
			}

			switch service.Version {
			// broadcast version means broadcast to all nodes
			case broadcastVersion:
				var success bool

				// publish to all nodes
				for _, node := range nodes {
					// publish async
					if err := pub(node, topic, b); err == nil {
						success = true
					}
				}

				// save if it failed to publish at least once
				if !success {
					h.saveMessage(topic, b)
				}
			default:
				// select node to publish to
				node := nodes[rand.Int()%len(nodes)]

				// publish async to one node
				if err := pub(node, topic, b); err != nil {
					// if failed save it
					h.saveMessage(topic, b)
				}
			}
		}
	}

	// do the rest async
	go func() {
		// get a third of the backlog
		// 从inbox中获取所有消息
		messages := h.getMessage(topic, 8)
		delay := (len(messages) > 1)

		// publish all the messages
		for _, msg := range messages {
			// serialize here
			// 调用srv方法将每条消息推送到服务节点去处理
			// 所以，如果发布事件方未主动再次触发 Publish 方法，则堆积在 inbox 中的消息不会被订阅方拉取消费。
			srv(s, msg)

			// sending a backlog of messages
			if delay {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	return nil
}

func (h *httpBroker) Subscribe(topic string, handler Handler, opts ...SubscribeOption) (Subscriber, error) {
	var err error
	var host, port string
	options := NewSubscribeOptions(opts...)

	// parse address for host, port
	// 这里解析出IP和端口号，通过IP和端口号来识别订阅了topic的进程
	host, port, err = net.SplitHostPort(h.Address())
	if err != nil {
		return nil, err
	}

	addr, err := maddr.Extract(host)
	if err != nil {
		return nil, err
	}

	var secure bool

	if h.opts.Secure || h.opts.TLSConfig != nil {
		secure = true
	}

	// register service
	node := &registry.Node{
		Id:      topic + "-" + h.id,
		Address: mnet.HostPort(addr, port),
		Metadata: map[string]string{
			"secure": fmt.Sprintf("%t", secure),
			"broker": "http",
			"topic":  topic,
		},
	}

	// check for queue group or broadcast queue
	version := options.Queue
	if len(version) == 0 {
		version = broadcastVersion
	}

	service := &registry.Service{
		Name:    serviceName,
		Version: version,
		Nodes:   []*registry.Node{node},
	}

	// generate subscriber
	// 首先，将启动的HTTP消息系统服务节点信息封装后设置到httpSubscriber实例中
	// 包含了订阅主题、传入 Subscribe 方法的事件处理器、当前的 httpBroker 实例等信息
	subscriber := &httpSubscriber{
		opts:  options,
		hb:    h,
		id:    node.Id,
		topic: topic,
		fn:    handler,
		svc:   service,
	}

	// subscribe now
	// 该方法会调用 h.r.Register 注册 "topic:" + topic 对应的 HTTP 消息系统服务节点信息，
	// 并将传入的 httpSubscriber 实例追加到 h.subscribers[s.topic] 数组中。
	if err := h.subscribe(subscriber); err != nil {
		return nil, err
	}

	// return the subscriber
	return subscriber, nil
}

func (h *httpBroker) String() string {
	return "http"
}

// NewBroker returns a new http broker
func NewBroker(opts ...Option) Broker {
	return newHttpBroker(opts...)
}
