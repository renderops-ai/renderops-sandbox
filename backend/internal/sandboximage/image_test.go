package sandboximage

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "default", want: Default},
		{name: "normalized", input: " Ubuntu-24.04 ", want: "ubuntu-24.04"},
		{name: "invalid separator", input: "ubuntu:latest", wantErr: true},
		{name: "invalid path", input: "../ubuntu", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Normalize(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Normalize(%q) error = %v, wantErr %t", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("Normalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestPoolKeySeparatesImageAndSize(t *testing.T) {
	if got := PoolKey("ubuntu", "2:512:10"); got != "ubuntu:2:512:10" {
		t.Fatalf("PoolKey() = %q", got)
	}
}

func TestSizeAllowed(t *testing.T) {
	cases := []struct {
		image, size string
		want        bool
	}{
		{Default, "nano", true},
		{Default, "medium", true},
		{Default, "large", false},
		{"agents", "large", true},
		{"agents", "nano", false},
		{"custom-image", "large", true}, // unknown images are left to worker routing
	}
	for _, c := range cases {
		if got := SizeAllowed(c.image, c.size); got != c.want {
			t.Errorf("SizeAllowed(%q, %q) = %v, want %v", c.image, c.size, got, c.want)
		}
	}
}
