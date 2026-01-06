package ledger

import (
	"errors"
	"reflect"
	"testing"
)

func TestPlanDebit(t *testing.T) {
	shards := []shardBal{{0, 30}, {1, 0}, {2, 50}, {3, 20}}
	tests := []struct {
		name      string
		amount    int64
		overdraft bool
		want      []leg
		err       error
	}{
		{"single shard covers", 10, false, []leg{{0, 10}}, nil},
		{"spans shards, skips empty", 60, false, []leg{{0, 30}, {2, 30}}, nil},
		{"exact total", 100, false, []leg{{0, 30}, {2, 50}, {3, 20}}, nil},
		{"insufficient", 101, false, nil, ErrInsufficientFunds},
		{"overdraft takes one shard", 1000, true, []leg{{0, 1000}}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := planDebit(shards, tc.amount, tc.overdraft)
			if !errors.Is(err, tc.err) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("planDebit(%d) = %v, %v; want %v, %v", tc.amount, got, err, tc.want, tc.err)
			}
			var sum int64
			for _, l := range got {
				sum += l.amount
			}
			if err == nil && sum != tc.amount {
				t.Fatalf("legs sum to %d, want %d", sum, tc.amount)
			}
		})
	}
}

func TestPlanDebitNegativeShard(t *testing.T) {
	// A shard can't be negative for a non-overdraft account, but if one were,
	// it must never be debited further.
	got, err := planDebit([]shardBal{{0, -5}, {1, 10}}, 10, false)
	if err != nil || !reflect.DeepEqual(got, []leg{{1, 10}}) {
		t.Fatalf("got %v, %v", got, err)
	}
}
