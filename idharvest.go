// Package idharvest contains features to harvest data from idporten.
//
// Harvesting Strategy
//
// Build initial harvest
//
// Read from both accounts from 2013 - 2020 merging the result into a cohesive count,
// establish tables in bigquery and stream the data row by row.
//
//	* Query eldest datasource.
//
// Streaming stratgy
//
// Get the last timestamp from the database and pull data from that point using
// the timestamp as a key to prevent duplicates.
//
// Validation strategy
//
// Previsously reported monthly data.
//

package idharvest

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Converts  a timestamp to the format 2014-05-01T20:00:00Z.
// Time needs to be in UTC
func DateToString(timestamp time.Time) string {
	if timestamp.Location() != time.UTC {
		log.Println("Warning: time called with a non UTC.locale.")
	}
	return timestamp.Format(time.RFC3339)
}

// Reads a timestamp of the format 2014-05-01T20:00:00Z and coverts it to a date.
func StringToDate(timestamp string) time.Time {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return t
}

// Org proides type protection to organization numbers, use the constants
// OrgNr or OldOrg when reading data.
type Org string

// OrgNr was used from 2013 to today.
var OrgNr Org = "889640782" // Fra 2013 - d.d.

// OldOrg was used  april 2018 - mai 2020
var OldOrg Org = "990983291"

// Query reads from the API and returns an array of Statistikk.
func Query(from time.Time, to time.Time, orgnum Org) (stat []Statistikk, err error) {

	stat = make([]Statistikk, 0)
	queryURL := "https://statistikk-utdata.difi.no/991825827/idporten-innlogging/hours" +
		"?from=" + DateToString(from) + "&" +
		"to=" + DateToString(to) + "&" +
		"categories=TE-orgnum=" + string(orgnum)
	res, err := http.Get(queryURL)
	if err != nil {
		return
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	json.Unmarshal(body, &stat)
	return
}
