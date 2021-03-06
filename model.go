package idharvest

import (
	"log"
	"time"
)

// Statistikk contains the API result with mapping to It was initially generated
// automaticly from the json-results of a REST-call last automated update on 23.
// november 2020. The field sum was added as a convenience for reporting;
// however this approach will fail when a new method of authentication is added
// to the reporting. Statistikk represents a single point of meassurement, while
// the result is an array of Statistikk:
//
//		stat := make([]Statistikk, 0)
//
// What do to when a new method of authentication is added
//
// BigQuery is immutable on data in the streaming buffer, the change strategy is
// to add the field and later make changes to historical data a few days later.
//
type Statistikk struct {
	Timestamp    time.Time `json:"timestamp" bigquery:"timestamp"`
	Measurements struct {
		MinIDPassport   int `json:"MinID passport" bigquery:"minid_passport"`
		Commfides       int `json:"Commfides" bigquery:"comfides"`
		BuypassPassport int `json:"Buypass passport" bigquery:"buypass_passport"`
		EIDAS           int `json:"eIDAS" bigquery:"eidas"`
		MinID           int `json:"MinID" bigquery:"minid"`
		BankIDMobil     int `json:"BankID mobil" bigquery:"bankid_mobil"`
		MinIDOTC        int `json:"MinID OTC" bigquery:"minid_otc"`
		Antall          int `json:"Antall" bigquery:"antall"`
		BuyPass         int `json:"BuyPass" bigquery:"buypass"`
		MinIDPIN        int `json:"MinID PIN" bigquery:"minid_pin"`
		Federated       int `json:"Federated" bigquery:"federated"`
		BankID          int `json:"BankID" bigquery:"bankid"`
	} `json:"measurements" bigquery:"measurements"`
	Categories struct {
		TEOrgnum string `json:"TE-orgnum"`
	} `json:"categories" bigquery:"categories"`
	Sum int `json:"sum,omitempty" bigquery:"sum"` // Privat kategori for summering.
}

// ToMetrics splits a single Statistikk structure to
// one metric for each row.
func (s Statistikk) ToMetrics() (metrics []Metric) {
	metrics = make([]Metric, 0)
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    MinIDPassport,
		Antall:    s.Measurements.MinIDPassport,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    Commfides,
		Antall:    s.Measurements.Commfides,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    Buypass,
		Antall:    s.Measurements.BuyPass,
	})

	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    BuypassPassport,
		Antall:    s.Measurements.BuypassPassport,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    EIDAS,
		Antall:    s.Measurements.EIDAS,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    MinID,
		Antall:    s.Measurements.MinID,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    BankIDMobil,
		Antall:    s.Measurements.BankIDMobil,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    MinIDOTC,
		Antall:    s.Measurements.MinIDOTC,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    MinIDPIN,
		Antall:    s.Measurements.MinIDPIN,
	})
	metrics = append(metrics, Metric{
		Timestamp: s.Timestamp,
		Metode:    BankID,
		Antall:    s.Measurements.BankID,
	})
	return
}

// Add two columns statistics objects.
func (a Statistikk) Add(b Statistikk) (c Statistikk) {
	c = a
	c.Measurements.MinIDPassport += b.Measurements.MinIDPassport
	c.Measurements.Commfides += b.Measurements.Commfides
	c.Measurements.BuypassPassport += b.Measurements.BuypassPassport
	c.Measurements.EIDAS += b.Measurements.EIDAS
	c.Measurements.MinID += b.Measurements.MinID
	c.Measurements.BankIDMobil += b.Measurements.BankIDMobil
	c.Measurements.MinIDOTC += b.Measurements.MinIDOTC
	c.Measurements.Antall += b.Measurements.Antall
	c.Measurements.BuyPass += b.Measurements.BuyPass
	c.Measurements.MinIDPIN += b.Measurements.MinIDPIN
	c.Measurements.Federated += b.Measurements.Federated
	c.Measurements.BankID += b.Measurements.BankID
	if c.Timestamp.Year() < 1000 {
		log.Fatal("Feiled date ", c)
	}
	return c
}

// CalcSum calculate the sum f all authentication methods, ignoring
// federated numbers.
func (a Statistikk) CalcSum() (b Statistikk) {
	b = a
	tmp := a.Measurements
	b.Sum = tmp.MinIDPassport +
		tmp.Commfides +
		tmp.BuypassPassport +
		tmp.EIDAS +
		tmp.MinID +
		tmp.BankIDMobil +
		tmp.MinIDOTC +
		tmp.MinIDOTC +
		tmp.BuyPass +
		tmp.MinIDPIN +
		tmp.BankID
	if b.Timestamp.Year() < 1000 {
		log.Fatal("Feiled date ", b)
	}
	return b
}

const (
	MinIDPassport   = "MinIDPassport"
	Commfides       = "Commfides"
	BuypassPassport = "BuypassPassport"
	EIDAS           = "EIDAS"
	MinID           = "MinID"
	BankIDMobil     = "BankIDMobil"
	MinIDOTC        = "MinIDOTC"
	Buypass         = "Buypass"
	MinIDPIN        = "MinIDPIN"
	BankID          = "BankID"
)

type Metric struct {
	Timestamp time.Time `bigquery:"timestamp"`
	Metode    string    `bigquery:"metode"`
	Antall    int       `bigquery:"antall"`
}
