package cluster

import "testing"

func TestOperatingSystemConnectionResolverProvidesBoundaryDependencies(t *testing.T) {
	resolver := operatingSystemConnectionSettingResolver()
	if resolver.readCredentialFile == nil || resolver.localizeCredentialPath == nil ||
		resolver.lookupEnvironment == nil || resolver.parseConfig == nil {
		t.Fatal("operating-system connection resolver is incomplete")
	}
}

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
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	settings := Settings{Host: "primary", Role: "runnerfoundry", Direct: &Endpoint{Host: "direct"}}
	primary, err := primaryConnectionConfig(settings)
	if err != nil || primary.Host != "primary" {
		t.Fatalf("primary config = %#v, %v", primary, err)
	}
	direct, err := directConnectionConfig(settings)
	if err != nil || direct.Host != "direct" {
		t.Fatalf("direct config = %#v, %v", direct, err)
	}
}
