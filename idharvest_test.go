package idharvest

import (
	"encoding/json"
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
	}{
		{
			name: "Simple case",
			args: args{
				in0: time.Date(2014, 05, 01, 20, 0, 0, 0, time.Now().UTC().Location()),
			},
			want: "2014-05-01T20:00:00Z",
		},
		{
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

func TestQuery(t *testing.T) {
	stat, err := Query(time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 5, 2, 0, 0, 0, 0, time.UTC),
		OrgNr)
	if err != nil {
		t.Fatal("Failed to read data:", err)
	}
	if len(stat) < 10 {
		t.Error("Too few results")
	}
}

// TestUnmarshal validates the parsing of the structure with a result from May 2020.
func TestUnmarshal(t *testing.T) {
	// Test datastructure
	var testData []byte = []byte(`[{"timestamp":"2020-05-01T00:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":188,"MinID OTC":11,"Antall":4256,"BuyPass":2,"MinID PIN":0,"Federated":3904,"BankID":151},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T01:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":95,"MinID OTC":6,"Antall":2369,"BuyPass":3,"MinID PIN":0,"Federated":2174,"BankID":91},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T02:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":47,"MinID OTC":6,"Antall":1125,"BuyPass":0,"MinID PIN":1,"Federated":1022,"BankID":49},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T03:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":37,"MinID OTC":2,"Antall":910,"BuyPass":0,"MinID PIN":0,"Federated":846,"BankID":25},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T04:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":30,"MinID OTC":6,"Antall":857,"BuyPass":0,"MinID PIN":1,"Federated":782,"BankID":38},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T05:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":30,"MinID OTC":5,"Antall":527,"BuyPass":0,"MinID PIN":0,"Federated":475,"BankID":17},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T06:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":56,"MinID OTC":8,"Antall":1058,"BuyPass":2,"MinID PIN":0,"Federated":923,"BankID":69},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T07:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":110,"MinID OTC":5,"Antall":2029,"BuyPass":2,"MinID PIN":0,"Federated":1805,"BankID":107},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T08:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":274,"MinID OTC":12,"Antall":4635,"BuyPass":3,"MinID PIN":2,"Federated":4099,"BankID":245},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T09:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":1202,"MinID OTC":28,"Antall":12247,"BuyPass":8,"MinID PIN":0,"Federated":10228,"BankID":781},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T10:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":1101,"MinID OTC":48,"Antall":14404,"BuyPass":7,"MinID PIN":1,"Federated":12384,"BankID":863},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T11:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":1184,"MinID OTC":32,"Antall":18903,"BuyPass":10,"MinID PIN":3,"Federated":16823,"BankID":851},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T12:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":1054,"MinID OTC":36,"Antall":18207,"BuyPass":15,"MinID PIN":1,"Federated":16245,"BankID":856},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T13:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":995,"MinID OTC":36,"Antall":19310,"BuyPass":22,"MinID PIN":1,"Federated":17433,"BankID":823},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T14:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":1062,"MinID OTC":34,"Antall":19798,"BuyPass":17,"MinID PIN":0,"Federated":17952,"BankID":733},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T15:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":966,"MinID OTC":28,"Antall":13158,"BuyPass":10,"MinID PIN":1,"Federated":11432,"BankID":721},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T16:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":918,"MinID OTC":36,"Antall":10956,"BuyPass":9,"MinID PIN":0,"Federated":9376,"BankID":617},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T17:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":843,"MinID OTC":19,"Antall":7793,"BuyPass":7,"MinID PIN":1,"Federated":6334,"BankID":589},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T18:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":737,"MinID OTC":32,"Antall":10409,"BuyPass":10,"MinID PIN":0,"Federated":9051,"BankID":579},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T19:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":657,"MinID OTC":22,"Antall":7828,"BuyPass":8,"MinID PIN":1,"Federated":6714,"BankID":426},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T20:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":535,"MinID OTC":22,"Antall":8382,"BuyPass":7,"MinID PIN":2,"Federated":7401,"BankID":415},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T21:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":415,"MinID OTC":26,"Antall":6850,"BuyPass":8,"MinID PIN":2,"Federated":6091,"BankID":308},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T22:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":326,"MinID OTC":19,"Antall":5515,"BuyPass":3,"MinID PIN":0,"Federated":4878,"BankID":289},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-01T23:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":220,"MinID OTC":10,"Antall":3369,"BuyPass":4,"MinID PIN":0,"Federated":2951,"BankID":184},"categories":{"TE-orgnum":"889640782"}},{"timestamp":"2020-05-02T00:00:00Z","measurements":{"MinID passport":0,"Commfides":0,"Buypass passport":0,"eIDAS":0,"MinID":0,"BankID mobil":169,"MinID OTC":9,"Antall":7531,"BuyPass":3,"MinID PIN":0,"Federated":7200,"BankID":150},"categories":{"TE-orgnum":"889640782"}}]`)
	stat := make([]Statistikk, 0)
	err := json.Unmarshal(testData, &stat)
	if err != nil {
		t.Fatal("failed: ", err)
	}
	for _, v := range stat {
		if v.Timestamp.Year() != 2020 {
			t.Error("Incorrect year: ", v)
		}
		if v.Timestamp.Month() != 5 {
			t.Error("Incorrect month: ", v)
		}
	}

}
