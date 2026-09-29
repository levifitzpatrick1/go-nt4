package nt4

import (
	"context"
	"testing"
	"time"
)

func TestTeamNumberToAddress(t *testing.T) {
	tests := []struct {
		team     int
		expected string
	}{
		{2064, "10.20.64.2"},
		{254, "10.2.54.2"},
		{1, "10.0.1.2"},
		{9999, "10.99.99.2"},
		{10000, "10.100.0.2"},
	}

	for _, tt := range tests {
		got := TeamNumberToAddress(tt.team)
		if got != tt.expected {
			t.Errorf("TeamNumberToAddress(%d) = %q; want %q", tt.team, got, tt.expected)
		}
	}
}

func TestDefaultClientOptions(t *testing.T) {
	opts := DefaultClientOptions("10.20.64.2")
	if opts.ServerAddress != "10.20.64.2" {
		t.Errorf("got ServerAddress %q; want 10.20.64.2", opts.ServerAddress)
	}
	if opts.Port != DefaultPort {
		t.Errorf("got Port %d; want %d", opts.Port, DefaultPort)
	}
	if opts.ClientName != "go-nt4" {
		t.Errorf("got ClientName %q; want go-nt4", opts.ClientName)
	}
}

func TestTypeStringAndIDMapping(t *testing.T) {
	types := []struct {
		str string
		id  int
	}{
		{TypeBoolean, DataTypeBoolean},
		{TypeDouble, DataTypeDouble},
		{TypeInt, DataTypeInt},
		{TypeFloat, DataTypeFloat},
		{TypeString, DataTypeString},
		{TypeBooleanArray, DataTypeBooleanArray},
		{TypeDoubleArray, DataTypeDoubleArray},
		{TypeIntArray, DataTypeIntArray},
		{TypeFloatArray, DataTypeFloatArray},
		{TypeStringArray, DataTypeStringArray},
		{TypeRaw, DataTypeBinary},
	}

	for _, tt := range types {
		gotID := TypeStringToID(tt.str)
		if gotID != tt.id {
			t.Errorf("TypeStringToID(%q) = %d; want %d", tt.str, gotID, tt.id)
		}
		gotStr := TypeIDToString(tt.id)
		if gotStr != tt.str {
			t.Errorf("TypeIDToString(%d) = %q; want %q", tt.id, gotStr, tt.str)
		}
	}

	// Unknown string fallback
	if got := TypeStringToID("unknown_custom_type"); got != DataTypeBinary {
		t.Errorf("TypeStringToID(unknown) = %d; want %d", got, DataTypeBinary)
	}

	// Unknown ID fallback
	if got := TypeIDToString(9999); got != TypeRaw {
		t.Errorf("TypeIDToString(9999) = %q; want %q", got, TypeRaw)
	}
}

func TestClientBuilder(t *testing.T) {
	builder := NewClientBuilder().
		Team(2064).
		Port(5811).
		Name("custom-robot").
		DialTimeout(3 * time.Second).
		WriteTimeout(4 * time.Second).
		ReadTimeout(15 * time.Second).
		Retry(100*time.Millisecond, 5*time.Second).
		Keepalive(2 * time.Second)

	client, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer client.Close()

	if client.opts.ServerAddress != "10.20.64.2" {
		t.Errorf("expected ServerAddress 10.20.64.2, got %q", client.opts.ServerAddress)
	}
	if client.opts.Port != 5811 {
		t.Errorf("expected Port 5811, got %d", client.opts.Port)
	}
	if client.opts.ClientName != "custom-robot" {
		t.Errorf("expected ClientName custom-robot, got %q", client.opts.ClientName)
	}
	if client.opts.DialTimeout != 3*time.Second {
		t.Errorf("expected DialTimeout 3s, got %v", client.opts.DialTimeout)
	}
	if client.opts.WriteTimeout != 4*time.Second {
		t.Errorf("expected WriteTimeout 4s, got %v", client.opts.WriteTimeout)
	}
	if client.opts.ReadTimeout != 15*time.Second {
		t.Errorf("expected ReadTimeout 15s, got %v", client.opts.ReadTimeout)
	}
	if client.opts.RetryMin != 100*time.Millisecond || client.opts.RetryMax != 5*time.Second {
		t.Errorf("expected Retry (100ms, 5s), got (%v, %v)", client.opts.RetryMin, client.opts.RetryMax)
	}
	if client.opts.KeepaliveInterval != 2*time.Second {
		t.Errorf("expected KeepaliveInterval 2s, got %v", client.opts.KeepaliveInterval)
	}
}

func TestClientBuilderMethods(t *testing.T) {
	// Test Server() and EndpointURL()
	b := NewClientBuilder().
		Server("192.168.1.100").
		EndpointURL("ws://192.168.1.100:5810")

	c, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer c.Close()

	if c.opts.ServerAddress != "192.168.1.100" {
		t.Errorf("expected ServerAddress 192.168.1.100, got %q", c.opts.ServerAddress)
	}
	if c.opts.EndpointURL != "ws://192.168.1.100:5810" {
		t.Errorf("expected EndpointURL ws://192.168.1.100:5810, got %q", c.opts.EndpointURL)
	}

	// Test Options() override
	b2 := NewClientBuilder().Options(ClientOptions{
		ServerAddress: "10.0.0.1",
		ClientName:    "override-name",
	})
	c2, err := b2.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer c2.Close()

	if c2.opts.ServerAddress != "10.0.0.1" || c2.opts.ClientName != "override-name" {
		t.Errorf("unexpected options after override: %+v", c2.opts)
	}

	// Test invalid options error return
	b3 := NewClientBuilder().EndpointURL("http://not-ws")
	_, err = b3.Build()
	if err == nil {
		t.Fatal("expected error for non-ws URL")
	}

	// Test BuildAndStart error on invalid options
	ctx := context.Background()
	_, err = b3.BuildAndStart(ctx)
	if err == nil {
		t.Fatal("expected BuildAndStart error for invalid options")
	}

	// Test BuildAndStart with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	c4, err := NewClientBuilder().Server("127.0.0.1").BuildAndStart(canceledCtx)
	if err == nil {
		c4.Close()
		t.Fatal("expected error with canceled context")
	}

	// Test BuildAndStart with live context
	liveCtx, cancelLive := context.WithCancel(context.Background())
	defer cancelLive()
	c5, err := NewClientBuilder().Server("127.0.0.1").BuildAndStart(liveCtx)
	if err != nil {
		t.Fatalf("expected BuildAndStart to succeed with live context, got %v", err)
	}
	c5.Close()
}
