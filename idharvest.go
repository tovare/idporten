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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
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

var datasetName string = "idporten"
var tableName string = "nav"
var projectID = "homepage-961"

// SendEverythingToBigquery proocesses all historical data and sends it to BigQuery.
// This process may take a few minutes and shuold be called locally. This procedure
// is destructive to all historical data.
//
// Preparing BigQuery
//
// A new dataset is created if it doens´t exist. A shema is inferred from the
// Statistikk struct and a a table is created for the data. If a table exist
// all data is deleted.
//
// Processing data
//
// All data is read from both organization numbers and merged using a map
// structure. Once complete all entries are extracted and and the array
// is sorted befre streaming the content to BigQuery.
//
func SendEverythingToBigquery() (err error) {

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return
	}
	defer client.Close()

	// Create a dataset if it doesn´t exist.
	if _, err := client.Dataset(datasetName).Metadata(ctx); err != nil {
		meta := &bigquery.DatasetMetadata{
			Description: "Statistikk om innlogginger fra idporten",
			Location:    "EU", // See https://cloud.google.com/bigquery/docs/locations
		}
		if err := client.Dataset(datasetName).Create(ctx, meta); err != nil {
			return err
		}
	}
	schema, err := bigquery.InferSchema(Statistikk{})
	if err != nil {
		return
	}
	metaData := &bigquery.TableMetadata{
		Schema:         schema,
		ExpirationTime: time.Now().AddDate(2, 0, 0), // Table will be automatically deleted in 2 years.
	}
	tableRef := client.Dataset(datasetName).Table(tableName)

	// Delete the table if it exists.
	_, err = tableRef.Metadata(ctx)
	if err == nil {
		if err := tableRef.Delete(ctx); err != nil {
			return err
		}
	}

	if err := tableRef.Create(ctx, metaData); err != nil {
		return err
	}

	largeSeries, err := Query(time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Now().In(time.UTC), OrgNr)
	if err != nil {
		return err
	}

	// Time needs to be in the same timezone since
	collatorMap := make(map[time.Time]Statistikk, 0)
	for _, v := range largeSeries {
		v := v.CalcSum()
		collatorMap[v.Timestamp] = v
		if v.Timestamp.Year() < 1000 {
			fmt.Println("Incorrect data in large series: ", v)
		}
	}

	smallSeries, err := Query(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 8, 1, 0, 0, 0, 0, time.UTC), OldOrg)
	for _, v := range smallSeries {
		t, ok := collatorMap[v.Timestamp]
		if ok {
			v = v.Add(t).CalcSum()
		}
		if v.Timestamp.Year() < 1000 {
			log.Fatal("Feiled date ", v)
		}
		collatorMap[v.Timestamp] = v
	}

	collatedSeries := make([]Statistikk, 0, len(collatorMap))
	for _, v := range collatorMap {
		collatedSeries = append(collatedSeries, v)
	}

	sort.Slice(collatedSeries, func(i, j int) bool {
		return collatedSeries[i].Timestamp.Before(collatedSeries[j].Timestamp)
	})

	fmt.Printf("Sucessfully processed %v lines ", len(collatedSeries))
	fmt.Println("First object is", collatedSeries[0].Timestamp)
	fmt.Println("Last object is", collatedSeries[len(collatedSeries)-1])

	/*
		if err := tableRef.Inserter().Put(ctx, collatedSeries); err != nil {
			return err
		}*/

	return err
}
