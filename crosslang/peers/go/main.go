// The Go participant of protobus-cpp's cross-language suite.
//
//	gopeer server   serve the interop services (protobus-go's own gopeer
//	                package); prints READY
//	gopeer client   run the client scenario against PEER_TARGET's services;
//	                prints PASS/FAIL lines, then DONE
//
// Built against the protobus-go checkout at PROTOBUS_GO (the suite rewrites
// go.mod's replace directive to point there). The broker is
// PROTOBUS_TEST_AMQP. The client mirrors protobus-go's goClientScenario.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/crosslang/gen/interop"
	"github.com/ArielLaub/protobus-go/v2/crosslang/gopeer"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

func dial() *protobus.Bus {
	cfg := protobus.DefaultConfig()
	cfg.RPCTimeout = 20 * time.Second
	cfg.StreamIdleTimeout = 20 * time.Second
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("PROTOBUS_TEST_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bus, err := protobus.Dial(ctx, os.Getenv("PROTOBUS_TEST_AMQP"), protobus.WithConfig(cfg), protobus.WithLogger(logger))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return bus
}

func serve() {
	bus := dial()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if _, err := gopeer.Serve(ctx, bus); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println("READY")
	<-ctx.Done()
	_ = bus.Close()
}

var failures int

func check(name string, fn func() error) {
	if err := fn(); err != nil {
		failures++
		fmt.Printf("FAIL %s: %s\n", name, strings.ReplaceAll(err.Error(), "\n", " | "))
		return
	}
	fmt.Printf("PASS %s\n", name)
}

func callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func client() {
	target := os.Getenv("PEER_TARGET")
	bus := dial()
	defer func() { _ = bus.Close() }()
	counter := interop.NewCounterClient(bus)
	wallet := interop.NewWalletClient(bus)

	check("unary", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		out, err := counter.Add(ctx, &interop.AddRequest{A: 2, B: 3})
		if err != nil || out.Sum != 5 {
			return fmt.Errorf("%v %v", out, err)
		}
		return nil
	})
	check("priority on a plain queue", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		_, err := counter.Add(ctx, &interop.AddRequest{A: 1}, protobus.WithPriority(2))
		return err
	})
	check("stream in order", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		var seqs []int32
		for tick, err := range counter.Tick(ctx, &interop.TickRequest{Count: 5}) {
			if err != nil {
				return err
			}
			if tick.Payload != fmt.Sprintf("chunk-%d", tick.Seq) {
				return fmt.Errorf("payload %q", tick.Payload)
			}
			seqs = append(seqs, tick.Seq)
		}
		if fmt.Sprint(seqs) != "[0 1 2 3 4]" {
			return fmt.Errorf("%v", seqs)
		}
		return nil
	})
	check("empty stream", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		for _, err := range counter.Tick(ctx, &interop.TickRequest{EmitNothing: true}) {
			return fmt.Errorf("unexpected item or error: %v", err)
		}
		return nil
	})
	check("mid-stream handled error", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		n := 0
		var last error
		for _, err := range counter.Tick(ctx, &interop.TickRequest{Count: 10, FailAt: 2}) {
			if err != nil {
				last = err
				break
			}
			n++
		}
		var re *protobus.RemoteError
		if n != 2 || !errors.As(last, &re) || re.Code != "TEST_FAIL" || !strings.Contains(re.Message, "deliberate failure at chunk 2") {
			return fmt.Errorf("%d chunks, %v", n, last)
		}
		return nil
	})
	check("mid-stream unhandled error", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		var last error
		for _, err := range counter.Tick(ctx, &interop.TickRequest{Count: 10, FailAt: 1, Unhandled: true}) {
			last = err
		}
		var re *protobus.RemoteError
		if !errors.As(last, &re) || re.Message != "stream broke" {
			return fmt.Errorf("%v", last)
		}
		return nil
	})
	check("cancellation reaches the producer", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		n := 0
		for _, err := range counter.Tick(ctx, &interop.TickRequest{Count: 500, DelayMs: 10}) {
			if err != nil {
				return err
			}
			if n++; n == 3 {
				break
			}
		}
		var p *interop.Produced
		for range 100 {
			var err error
			if p, err = counter.Produced(ctx, &interop.Nothing{}); err != nil {
				return err
			}
			if p.StoppedEarly {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !p.StoppedEarly || p.Finished || p.Yielded >= 500 {
			return fmt.Errorf("the %s producer never saw the cancellation: %v", target, p)
		}
		return nil
	})
	check("custom types, defaults and maps", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		got, err := wallet.Balance(ctx, &interop.Query{Account: "acc"})
		if err != nil {
			return err
		}
		if !proto.Equal(gopeer.Canonical(), got) {
			return fmt.Errorf("balance from %s differs: %v", target, got)
		}
		return nil
	})
	check("echo round trip", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		got, err := wallet.Echo(ctx, gopeer.Canonical())
		if err != nil {
			return err
		}
		if !proto.Equal(gopeer.Canonical(), got) {
			return fmt.Errorf("echo through %s differs: %v", target, got)
		}
		return nil
	})
	check("handled error", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		_, err := wallet.Balance(ctx, &interop.Query{Account: "boom"})
		var re *protobus.RemoteError
		if !errors.As(err, &re) || re.Code != "NOT_FOUND" || re.Message != "no such account" {
			return fmt.Errorf("%#v", err)
		}
		return nil
	})
	check("unhandled error", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		_, err := wallet.Balance(ctx, &interop.Query{Account: "crash"})
		var re *protobus.RemoteError
		if !errors.As(err, &re) || re.Message != "kaboom" {
			return fmt.Errorf("%#v", err)
		}
		return nil
	})
	check("unimplemented method", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		_, err := counter.Unimplemented(ctx, &interop.Nothing{})
		if !protobus.IsCode(err, "PROTOCOL_ERROR") {
			return fmt.Errorf("%v", err)
		}
		return nil
	})
	check("call metadata", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		who, err := counter.Whoami(ctx, &interop.Nothing{}, protobus.WithActor("client-go"), protobus.WithMessageID("mid-go-1"))
		if err != nil {
			return err
		}
		want := &interop.Who{Actor: "client-go", MessageId: "mid-go-1", RoutingKey: "REQUEST.interop.Counter.whoami", Lang: target}
		if !proto.Equal(who, want) {
			return fmt.Errorf("got %v want %v", who, want)
		}
		return nil
	})
	check("instance routing", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		who, err := interop.NewCounterClient(bus, protobus.WithInstance("inst1")).Whoami(ctx, &interop.Nothing{})
		if err != nil || who.RoutingKey != "REQUEST.interop.Counter.inst1.whoami" {
			return fmt.Errorf("%v %v", who, err)
		}
		return nil
	})
	check("events both ways", func() error {
		ctx, cancel := callCtx()
		defer cancel()
		l, err := bus.NewEventListener("")
		if err != nil {
			return err
		}
		defer func() { _ = l.Close() }()
		got := make(chan *interop.Ping, 1)
		err = protobus.Subscribe(ctx, l, func(_ context.Context, p *interop.Ping, _ protobus.EventInfo) error {
			select {
			case got <- p:
			default:
			}
			return nil
		}, protobus.WithTopic("EVENT.pong."+target))
		if err != nil {
			return err
		}
		if err := l.Start(ctx); err != nil {
			return err
		}
		n := new(big.Int).Lsh(big.NewInt(1), 70)
		b, _ := pbtypes.NewBigint(n)
		if err := bus.PublishEvent(ctx, &interop.Ping{Id: "go-1", N: b, From: "go"}, protobus.WithTopic("EVENT.ping."+target)); err != nil {
			return err
		}
		select {
		case p := <-got:
			v, _ := p.N.BigInt()
			if p.Id != "pong:go-1" || p.From != target || v.Cmp(new(big.Int).Add(n, big.NewInt(1))) != 0 {
				return fmt.Errorf("pong %v (n=%s)", p, v)
			}
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("no pong")
		}
	})
	fmt.Println("DONE")
	if failures > 0 {
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "server" {
		serve()
		return
	}
	client()
}
