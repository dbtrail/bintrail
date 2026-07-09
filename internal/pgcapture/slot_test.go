package pgcapture

import (
	"strings"
	"testing"
)

func TestRestrictedPublicationError(t *testing.T) {
	t.Run("empty is nil", func(t *testing.T) {
		if err := restrictedPublicationError("p", nil); err != nil {
			t.Fatalf("expected nil for no restricted tables, got %v", err)
		}
		if err := restrictedPublicationError("p", []restrictedTable{}); err != nil {
			t.Fatalf("expected nil for empty slice, got %v", err)
		}
	})

	t.Run("row filter fails loud", func(t *testing.T) {
		err := restrictedPublicationError("mypub", []restrictedTable{
			{name: "public.orders", hasFilter: true},
		})
		if err == nil {
			t.Fatal("expected an error for a row-filtered table")
		}
		msg := err.Error()
		for _, want := range []string{"mypub", "public.orders (row filter)", "SUBSET"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q missing %q", msg, want)
			}
		}
	})

	t.Run("column list fails loud", func(t *testing.T) {
		err := restrictedPublicationError("mypub", []restrictedTable{
			{name: "public.orders", hasColList: true},
		})
		if err == nil {
			t.Fatal("expected an error for a column-list table")
		}
		if !strings.Contains(err.Error(), "public.orders (column list)") {
			t.Errorf("error %q missing column-list reason", err.Error())
		}
	})

	t.Run("both reasons combined", func(t *testing.T) {
		err := restrictedPublicationError("mypub", []restrictedTable{
			{name: "public.orders", hasFilter: true, hasColList: true},
		})
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "public.orders (row filter + column list)") {
			t.Errorf("error %q missing combined reason", err.Error())
		}
	})

	t.Run("multiple tables sorted and named", func(t *testing.T) {
		err := restrictedPublicationError("mypub", []restrictedTable{
			{name: "public.zebra", hasFilter: true},
			{name: "public.apple", hasColList: true},
		})
		if err == nil {
			t.Fatal("expected an error")
		}
		msg := err.Error()
		ai := strings.Index(msg, "public.apple")
		zi := strings.Index(msg, "public.zebra")
		if ai < 0 || zi < 0 || ai > zi {
			t.Errorf("expected apple before zebra (sorted), got %q", msg)
		}
	})
}
