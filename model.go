package idharvest

import "time"

// Statistikk contains the API result with mapping to It was initially generated
// automaticly from the json-results of a REST-call last automated update on 23.
// november 2020. The field sum was added as a convenience for reporting;
// however this approach will fail when a new method of authentication is added
// to the reporting.
//
// What do to when a new method of authentication is added
//
// BigQuery is immutable on data in the streaming buffer, the change strategy is
// to add the field and later make changes to historical data a few days later.
//
type Statistikk []struct {
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
