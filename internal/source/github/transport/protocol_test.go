package transport

import (
	"io"
	"net/http"
	"strings"
)

type protocolResponse struct {
	status int
	body   string
}

type protocolTransport struct {
	responses map[string]protocolResponse
	err       error
}

func (transport *protocolTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.err != nil {
		return nil, transport.err
	}
	configured := transport.responses[request.URL.Path]
	return &http.Response{
		StatusCode: configured.status,
		Status:     http.StatusText(configured.status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(configured.body)),
		Request:    request,
	}, nil
}
