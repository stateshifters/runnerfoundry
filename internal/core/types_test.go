package core

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestSourceReferenceValidity(t *testing.T) {
	if !(SourceReference{}).IsEmpty() || (SourceReference{}).IsValid() {
		t.Fatal("empty source reference classification is inconsistent")
	}
	valid := SourceReference{Adapter: "github", ID: "request"}
	if valid.IsEmpty() || !valid.IsValid() {
		t.Fatal("complete source reference classification is inconsistent")
	}
	partial := SourceReference{Adapter: "github"}
	if partial.IsEmpty() || partial.IsValid() {
		t.Fatal("partial source reference classification is inconsistent")
	}
}

func TestDemandScopeValidity(t *testing.T) {
	valid := DemandScope{Adapter: "github", ID: "repository-pool"}
	if !valid.IsValid() {
		t.Fatal("complete demand scope is invalid")
	}
	for _, invalid := range []DemandScope{{}, {Adapter: "github"}, {ID: "repository-pool"}} {
		if invalid.IsValid() {
			t.Fatalf("DemandScope %#v is valid", invalid)
		}
	}
}

func TestJobIdentityValidation(t *testing.T) {
	if err := (JobIdentity{Source: SourceReference{Adapter: "github", ID: "request"}}).Validate(); err != nil {
		t.Fatalf("source identity validation: %v", err)
	}
	if err := (JobIdentity{JobID: uuid.New()}).Validate(); err != nil {
		t.Fatalf("job identity validation: %v", err)
	}
	for _, invalid := range []JobIdentity{{}, {Source: SourceReference{ID: "request"}}} {
		if err := invalid.Validate(); !errors.Is(err, ErrInvalidJobIdentity) {
			t.Fatalf("invalid identity error = %v", err)
		}
	}
}
