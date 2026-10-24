package orchard

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type vmService interface {
	Create(ctx context.Context, vm *v1.VM) error
	List(ctx context.Context, options ...client.ListOption) ([]v1.VM, error)
	Delete(ctx context.Context, name string) error
	PortForwardTarget(ctx context.Context, name string, target client.PortForwardTarget, waitSeconds uint16) (net.Conn, error)
}

type SDKAPI struct{ vms vmService }

func (api SDKAPI) Create(ctx context.Context, vm *v1.VM) error {
	return sdkError(api.vms.Create(ctx, vm))
}
func (api SDKAPI) List(ctx context.Context) ([]v1.VM, error) {
	machines, err := api.vms.List(ctx)
	return machines, sdkError(err)
}
func (api SDKAPI) Delete(ctx context.Context, name string) error {
	err := api.vms.Delete(ctx, name)
	var apiError *client.APIError
	if errors.As(err, &apiError) && apiError.StatusCode == http.StatusNotFound {
		return nil
	}
	return sdkError(err)
}

func NewClient(config Config) (*SDKAPI, *SDKConnector, error) {
	address, err := normalizeURL(config.ControllerURL)
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(config.ServiceAccount) == "" || strings.TrimSpace(config.Token) == "" {
		return nil, nil, errors.New("orchard service account and token are required")
	}
	options := []client.Option{client.WithAddress(address), client.WithCredentials(config.ServiceAccount, strings.TrimSpace(config.Token))}
	if len(config.TrustedCertificate) > 0 {
		certificate, certificateError := trustedCertificate(config.TrustedCertificate)
		if certificateError != nil {
			return nil, nil, certificateError
		}
		options = append(options, client.WithTrustedCertificate(certificate))
	}
	sdk, err := client.New(options...)
	return &SDKAPI{vms: sdk.VMs()}, &SDKConnector{vms: sdk.VMs(), newConnection: newGuestConnection}, sdkError(err)
}

func trustedCertificate(content []byte) (*x509.Certificate, error) {
	block, rest := pem.Decode(content)
	if block == nil || block.Type != "CERTIFICATE" || strings.TrimSpace(string(rest)) != "" {
		return nil, errors.New("invalid Orchard trusted certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid Orchard trusted certificate")
	}
	return certificate, nil
}

type SDKConnector struct {
	vms           vmService
	newConnection func(dialer func(context.Context, string) (net.Conn, error)) (Guest, error)
}

func (connector SDKConnector) Connect(ctx context.Context, name string) (Guest, error) {
	guestContext, cancelGuest := context.WithCancel(ctx)
	guest, err := connector.newConnection(func(dialContext context.Context, _ string) (net.Conn, error) {
		return connector.forwardGuest(guestContext, dialContext, name)
	})
	if err != nil {
		cancelGuest()
		return Guest{}, err
	}
	closeConnection := guest.Close
	guest.Close = func() error { cancelGuest(); return closeConnection() }
	return guest, nil
}

func (connector SDKConnector) forwardGuest(guestContext context.Context, dialContext context.Context, name string) (net.Conn, error) {
	forwardContext, cancelForward := context.WithCancel(guestContext)
	stopDialCancellation := context.AfterFunc(dialContext, cancelForward)
	connection, err := connector.vms.PortForwardTarget(forwardContext, name, client.PortForwardTargetTartGuestAgent, 60)
	if !stopDialCancellation() {
		cancelForward()
	}
	err = errors.Join(err, forwardContext.Err())
	if err != nil {
		cancelForward()
		if connection != nil {
			err = errors.Join(err, connection.Close())
		}
		return nil, sdkError(err)
	}
	return &forwardedConnection{Conn: connection, cancel: cancelForward}, nil
}

type forwardedConnection struct {
	net.Conn
	cancel context.CancelFunc
}

func (connection *forwardedConnection) Close() error {
	connection.cancel()
	return sdkError(connection.Conn.Close())
}
func newGuestConnection(dialer func(context.Context, string) (net.Conn, error)) (Guest, error) {
	connection, err := grpc.NewClient("passthrough:///tart-guest-agent", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStaticStreamWindowSize(64*1024), grpc.WithStaticConnWindowSize(64*1024), grpc.WithContextDialer(dialer))
	return rpcGuest(guestagent.NewAgentClient(connection).Exec, connection.Close), sdkError(err)
}
func rpcGuest(openStream func(context.Context, ...grpc.CallOption) (guestagent.Agent_ExecClient, error), closeConnection func() error) Guest {
	return Guest{Exec: func(ctx context.Context) (ExecStream, error) {
		stream, err := openStream(ctx)
		if err != nil {
			return ExecStream{}, sdkError(err)
		}
		return ExecStream{Send: stream.Send, CloseSend: stream.CloseSend, Recv: stream.Recv}, nil
	}, Close: func() error { return sdkError(closeConnection()) }}
}
func sdkError(err error) error {
	if err != nil {
		return errors.New("orchard SDK operation failed")
	}
	return nil
}
