package cluster

import "testing"

func TestOperatingSystemCredentialPathAcceptsAbsoluteAndRejectsRelativePaths(t *testing.T) {
	localized, err := operatingSystemCredentialPath("//server/share/credential")
	if err != nil || localized == "" {
		t.Fatalf("localized credential path = %q, %v", localized, err)
	}
	if _, err := operatingSystemCredentialPath("relative/credential"); err == nil {
		t.Fatal("expected relative credential path rejection")
	}
}

func TestProductionConnectionEntryPointsUseConfiguredResolver(t *testing.T) {
	resolver := testConnectionResolver(nil)
	settings := Settings{Host: "primary", Role: "runnerfoundry", Direct: &Endpoint{Host: "direct"}}
	primary, err := primaryConnectionConfigWithResolver(settings, resolver)
	if err != nil || primary.Host != "primary" {
		t.Fatalf("primary config = %#v, %v", primary, err)
	}
	direct, err := directConnectionConfigWithResolver(settings, resolver)
	if err != nil || direct.Host != "direct" {
		t.Fatalf("direct config = %#v, %v", direct, err)
	}
}
