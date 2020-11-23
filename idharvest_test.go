package idharvest

import (
	"reflect"
	"testing"
	"time"
)

func TestStringToDate(t *testing.T) {
	type args struct {
		timestamp string
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{{
		name: "Simple case",
		args: args{
			timestamp: "2014-05-01T20:00:00Z",
		},
		want: time.Date(2014, 05, 01, 20, 0, 0, 0, time.Now().UTC().Location())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringToDate(tt.args.timestamp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringToDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDateToString(t *testing.T) {
	type args struct {
		in0 time.Time
	}
	tests := []struct {
		name string
		args args
		want string
	}{{
		name: "Simple case",
		args: args{
			in0: time.Date(2014, 05, 01, 20, 0, 0, 0, time.Now().UTC().Location()),
		},
		want: "2014-05-01T20:00:00Z",
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DateToString(tt.args.in0); got != tt.want {
				t.Errorf("DateToString() = %v, want %v", got, tt.want)
			}
		})
	}
}
