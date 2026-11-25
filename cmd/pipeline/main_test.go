package main

import "testing"

func TestParseRole(t *testing.T) {
	cases := map[string]roles{
		"all":    {true, true, true},
		"ingest": {ingest: true},
		"api":    {api: true},
		"alerts": {alerts: true},
	}
	for in, want := range cases {
		got, err := parseRole(in)
		if err != nil || got != want {
			t.Errorf("parseRole(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	if _, err := parseRole("writer"); err == nil {
		t.Error("expected an error for an unknown role")
	}
}
