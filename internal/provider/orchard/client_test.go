package orchard

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
	"google.golang.org/grpc"
)

func TestSDKDeletionOnlyTreatsAuthenticatedNotFoundAsAbsent(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		absent bool
	}{{"accepted", nil, true}, {"absent", &client.APIError{StatusCode: http.StatusNotFound}, true}, {"unauthorized", &client.APIError{StatusCode: http.StatusUnauthorized}, false}, {"forbidden", &client.APIError{StatusCode: http.StatusForbidden}, false}, {"server unavailable", &client.APIError{StatusCode: http.StatusServiceUnavailable}, false}, {"transport uncertainty", errors.New("transport"), false}} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeVMService{deleteError: test.err}
			api := SDKAPI{vms: service}
			err := api.Delete(context.Background(), "owned")
			if (err == nil) != test.absent || len(service.deleted) != 1 || service.deleted[0] != "owned" {
				t.Fatalf("delete=%v", err)
			}
		})
	}
	service := &fakeVMService{records: []v1.VM{ownedVM("cG9vbA")}}
	api := SDKAPI{vms: service}
	vm := ownedVM("b3RoZXI")
	if err := api.Create(context.Background(), &vm); err != nil || service.created[0] != &vm {
		t.Fatal("SDK creation lost native spec")
	}
	if instances, err := api.List(context.Background()); err != nil || len(instances) != 1 || service.lists != 1 {
		t.Fatalf("list=%+v error=%v", instances, err)
	}
}

func TestClientConstructionValidatesCredentialsAndCertificateWithoutExternalIO(t *testing.T) {
	cfg := Config{ControllerURL: "https://orchard.example", ServiceAccount: "runnerfoundry", Token: "token\n"}
	api, connector, err := NewClient(cfg)
	if err != nil || api == nil || connector == nil {
		t.Fatalf("construction=%v", err)
	}
	for _, change := range []func(*Config){func(cfg *Config) { cfg.ControllerURL = "invalid" }, func(cfg *Config) { cfg.ServiceAccount = "" }, func(cfg *Config) { cfg.Token = " \n" }, func(cfg *Config) { cfg.TrustedCertificate = []byte("invalid") }, func(cfg *Config) {
		cfg.TrustedCertificate = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("invalid")})
	}, func(cfg *Config) {
		cfg.TrustedCertificate = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("invalid")})
	}} {
		invalid := cfg
		change(&invalid)
		if _, _, constructionError := NewClient(invalid); constructionError == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestClientConstructionAcceptsOnlySingleValidTrustedCertificate(t *testing.T) {
	cfg := Config{ControllerURL: "https://orchard.example", ServiceAccount: "runnerfoundry", Token: "token"}
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"orchard.example"}, NotBefore: time.Unix(0, 0), NotAfter: time.Unix(2000000000, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	certificate, err := x509.CreateCertificate(bytes.NewReader(make([]byte, 64)), template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cfg.TrustedCertificate = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})
	if _, _, err := NewClient(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.TrustedCertificate = append(cfg.TrustedCertificate, []byte("extra")...)
	if _, _, err := NewClient(cfg); err == nil {
		t.Fatal("trailing certificate data accepted")
	}
}

func TestSDKGuestUsesOrchardTartGuestAgentTarget(t *testing.T) {
	service := &fakeVMService{forwardError: errors.New("forward boundary")}
	connector := SDKConnector{vms: service, newConnection: func(dialer func(context.Context, string) (net.Conn, error)) (Guest, error) {
		_, err := dialer(context.Background(), "ignored")
		if err == nil || service.forwardName != "owned" || service.forwardTarget != client.PortForwardTargetTartGuestAgent || service.waitSeconds != 60 {
			t.Fatalf("forward=%+v error=%v", service, err)
		}
		return Guest{}, err
	}}
	if _, err := connector.Connect(context.Background(), "owned"); err == nil {
		t.Fatal("forward failure accepted")
	}
}

func TestRPCGuestOpensExecStreamAndClosesConnection(t *testing.T) {
	called := false
	closed := false
	guest := rpcGuest(func(context.Context, ...grpc.CallOption) (guestagent.Agent_ExecClient, error) {
		called = true
		return &grpc.GenericClientStream[guestagent.ExecRequest, guestagent.ExecResponse]{}, nil
	}, func() error { closed = true; return nil })
	stream, err := guest.Exec(context.Background())
	if err != nil || !called || stream.Send == nil || stream.Recv == nil || stream.CloseSend == nil {
		t.Fatalf("execution=%+v error=%v", stream, err)
	}
	if closeError := guest.Close(); closeError != nil || !closed {
		t.Fatal("connection not closed")
	}
}

func TestRPCGuestRejectsExecutionAndCloseFailures(t *testing.T) {
	guest := rpcGuest(func(context.Context, ...grpc.CallOption) (guestagent.Agent_ExecClient, error) {
		return nil, errors.New("execution failure")
	}, func() error { return errors.New("close failure") })
	if _, err := guest.Exec(context.Background()); err == nil {
		t.Fatal("execution failure accepted")
	}
	if err := guest.Close(); err == nil {
		t.Fatal("close failure accepted")
	}
}

func TestGuestConnectionConstructionDoesNotDialNetwork(t *testing.T) {
	connection, err := newGuestConnection(func(context.Context, string) (net.Conn, error) {
		t.Fatal("lazy gRPC construction dialed network")
		return nil, errors.New("unexpected dial")
	})
	if err != nil {
		t.Fatal(err)
	}
	if closeError := connection.Close(); closeError != nil {
		t.Fatal(closeError)
	}
}

type fakeVMService struct {
	fakeAPI
	forwardError  error
	forwardName   string
	forwardTarget client.PortForwardTarget
	waitSeconds   uint16
	onForward     func(ctx context.Context) (net.Conn, error)
}

func (service *fakeVMService) List(ctx context.Context, _ ...client.ListOption) ([]v1.VM, error) {
	return service.fakeAPI.List(ctx)
}
func (service *fakeVMService) PortForwardTarget(ctx context.Context, name string, target client.PortForwardTarget, wait uint16) (net.Conn, error) {
	service.forwardName = name
	service.forwardTarget = target
	service.waitSeconds = wait
	if service.onForward != nil {
		return service.onForward(ctx)
	}
	return nil, service.forwardError
}
