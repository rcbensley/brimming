package main

import "testing"

func TestStrToFloat(t *testing.T) {
	expect := map[string]float64{
		"100mb": 100.0,
		"123gb": 123.0,
		"0abc":  0,
	}

	for ek, ev := range expect {
		got, _ := sizeToFloat(ek)
		if got != ev {
			t.Fatalf("Expected %v from %s, got %v", ev, ek, got)
		}
	}
}

func TestSizeToRows(t *testing.T) {
	expect := map[string]int64{
		"2 thing": 0,
		"100mb":   100000,
		"123gb":   123000000,
		"7.8tb":   7800000000,
		"2tb":     2000000000,
		"12kb":    0,
		"mb":      0,
		"456":     0,
	}

	for ek, ev := range expect {
		got, _ := sizeToRows(ek)
		if ev != got {
			t.Fatalf("Expected %v from %s, got %v", ev, ek, got)
		}
	}

}

func TestNewBrim(t *testing.T) {
}

func TestGenerateJobs(t *testing.T) {
}
