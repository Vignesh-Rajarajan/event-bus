package web

import "testing"

func TestValidCategory(t *testing.T) {
	testCases := []struct {
		category string
		valid    bool
	}{
		{
			category: "",
			valid:    false,
		},
		{
			category: ".",
			valid:    false,
		},
		{
			category: "..",
			valid:    false,
		},
		{
			category: "number",
			valid:    true,
		},
		{
			category: "num\nbers",
			valid:    true,
		},
		{
			category: "_:num\nbe:rs",
			valid:    true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.category, func(t *testing.T) {
			got := isValidCategory(tc.category)
			if got != tc.valid {
				t.Errorf("got %v want %v", got, tc.valid)
			}
		})
	}
}
