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

import "time"

// Converts  a timestamp to the format 2014-05-01T20:00:00Z.
func DateToString(timestamp time.Time) string {
	return timestamp.Format(time.RFC3339)

}

// Reads a timestamp of the format 2014-05-01T20:00:00Z and coverts it to a date.
func StringToDate(timestamp string) time.Time {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return t
}

// https://statistikk-utdata.difi.no/991825827/idporten-innlogging/hours?from=2014-05-01T00:00:00Z&to=2014-05-02T23:59:59Z&categories=TE-orgnum=889640782
