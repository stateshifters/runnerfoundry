package orchard

import (
	"context"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"testing"
)

func TestNativeSpecPreservesImplicitTartRuntimeAndGuestStartup(t *testing.T) {
	_, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(`{"image":"base","startup_script":{"transport":"tart-guest-agent","script_content":"prepare"}}`)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNativeSpecRejectsUnsupportedEnumValues(t *testing.T) {
	for _, spec := range []string{`{"image":"base","restart_policy":"never"}`, `{"image":"base","restart_policy":"invalid"}`, `{"image":"base","imagePullPolicy":"invalid"}`, `{"image":"base","startup_script":{"transport":"invalid"}}`} {
		if _, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(spec)}); err == nil {
			t.Fatalf("accepted %s", spec)
		}
	}
	for _, spec := range []string{`{"image":"base","restart_policy":"Never","imagePullPolicy":"IfNotPresent"}`, `{"image":"base","restart_policy":"OnFailure","imagePullPolicy":"Always"}`} {
		if _, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: "https://[::1]:443/", VMSpec: []byte(spec)}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeSpecRejectsInvalidEndpointHostProcessAndHostDirectoryDeclarations(t *testing.T) {
	for _, spec := range []string{
		`{"image":"base","endpoints":[{}]}`,
		`{"image":"base","hostProcesses":[{}]}`,
		`{"image":"base","hostDirs":[{"path":"/share"}]}`,
		`{"image":"base","hostDirs":[{"name":"share"}]}`,
	} {
		t.Run(spec, func(t *testing.T) {
			if _, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(spec)}); err == nil {
				t.Fatal("invalid native declaration accepted")
			}
		})
	}
	valid := `{"image":"base","endpoints":[{"name":"ssh","protocol":"tcp","target":{"vm":{"port":22}}}],"hostProcesses":[{"name":"proxy","program":"proxy","args":["--port","8080"]}],"hostDirs":[{"name":"share","path":"/share","ro":true}]}`
	if _, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(valid)}); err != nil {
		t.Fatal(err)
	}
}

func TestSpecRejectsInvalidOrServerOwnedFields(t *testing.T) {
	for _, spec := range []string{`null`, `[]`, `{}`, `{"image":"base","unknown":true}`, `{"image":"base","name":"owned"}`, `{"image":"base","Name":"owned"}`, `{"image":"base","status":"running"}`, `{"image":"base","createdAt":null}`, `{"image":"base","generation":0}`, `{"image":"base","observedGeneration":0}`, `{"image":"base","runtime":"vetu"}`, `{"image":"base","os":"linux"}`, `{"image":"base","arch":"amd64"}`, `{"image":"base","powerState":"stopped"}`, `{"image":"base","cpu":"invalid"}`, `{"image":"base","startup_script":{"unknown":true}}`, `{"image":"base"} {}`} {
		t.Run(spec, func(t *testing.T) {
			if _, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(spec)}); err == nil {
				t.Fatal("invalid spec accepted")
			}
		})
	}
}

func TestControllerURLIdentityIsIndependentOfConfigurationAndMember(t *testing.T) {
	for _, address := range []string{"https://orchard.example:443/", "HTTPS://ORCHARD.EXAMPLE", "https://orchard.example///"} {
		provider, err := NewProvider(&fakeAPI{records: []v1.VM{}}, nil, Config{ControllerURL: address, VMSpec: []byte(`{"image":"base"}`)})
		if err != nil || provider.EndpointID() != "orchard:https://orchard.example" {
			t.Fatalf("provider=%+v error=%v", provider, err)
		}
		if err := provider.Validate(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, address := range []string{"", "example.com", "ftp://example.com", "http://user:password@example.com", "https://example.com?query=x", "https://example.com#fragment", "https://%"} {
		if _, err := NewProvider(&fakeAPI{}, nil, Config{ControllerURL: address, VMSpec: []byte(`{"image":"base"}`)}); err == nil {
			t.Fatalf("accepted %q", address)
		}
	}
}
