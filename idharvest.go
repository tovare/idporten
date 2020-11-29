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

const (
	datasetName      string = "idporten"
	tableName        string = "nav"
	projectID        string = "	"
	MetricsTableName string = "navmetrics"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// StreamLatestDataToBigQuery incrementally updates BigQuery with the most
// recent data as a cloud function. An hourly update of the most recent data
// from idporten.
//
// Checks to see the most recent entry in BigQuery. Makes a query for the most
// recent data and streams it into BigQuery.
//
//
//    gcloud functions deploy StreamLatestDataToBigQuery --memory=127 --runtime go113 --trigger-topic monitor
func StreamLatestDataToBigQuery(ctx context.Context, m PubSubMessage) (err error) {

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return
	}
	defer client.Close()
	// Query the last entry, this will return multiple lines, one for each metric.
	q := client.Query(`
		SELECT * FROM homepage-961.idporten.navmetrics WHERE (timestamp) IN 
			( SELECT MAX(timestamp) FROM homepage-961.idporten.navmetrics )
		`)

	it, err := q.Read(ctx)
	if err != nil {
		return
	}
	var values Metric
	// Will zero out values when reaching the end. We only need the first entry this time.
	err = it.Next(&values)
	if err != nil {
		return
	}

	// I assume we get so little data that we can gather it all in one go.
	// we could reload everything if discrepancies arise over time.
	fromTime := values.Timestamp.Add(time.Hour)
	toTime := time.Now().UTC()
	if fromTime.After(toTime) {
		// Sanity check failed. If we run collection too fast, we
		// shouldn´t do anyting.
		return
	}

	series, err := Query(fromTime, toTime, OrgNr)
	if err != nil {
		return err
	}

	metrics := make([]Metric, 0)
	for _, v := range series {
		metrics = append(metrics, v.ToMetrics()...)
	}

	// Stream to BigQuery tables.
	metricsTableRef := client.Dataset(datasetName).Table(MetricsTableName)
	if err := metricsTableRef.Inserter().Put(ctx, metrics); err != nil {
		return err
	}

	seriesTableRef := client.Dataset(datasetName).Table(tableName)
	if err := seriesTableRef.Inserter().Put(ctx, series); err != nil {
		return err
	}

	return
}

// SendEverythingToBigquery proocesses all historical data and sends it to BigQuery.
// This process may take a few minutes and shuold be called locally. This procedure
// is destructive to all historical data and shold only be used when rebuilding
// everything from
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
// Datastudio-friendly format
//
// Metrics are stred with one metric for each line, this makes some graphs
// work a lot better.
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

	log.Println("Slowly read the data from API to the large series.")
	largeSeries := make([]Statistikk, 0)
	{
		limiter := time.Tick(500 * time.Millisecond)
		fromDate := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
		toDate := time.Now().In(time.UTC)
		aDate := fromDate
		const MonthIncrement = 5
		for aDate.Before(toDate) {
			log.Printf("Reading from %v to %v", aDate, aDate.AddDate(0, MonthIncrement, 0))
			tmp, err := Query(aDate, aDate.AddDate(0, MonthIncrement, 0), OrgNr)
			if err != nil {
				return err
			}
			largeSeries = append(largeSeries, tmp...)
			<-limiter
			aDate = aDate.AddDate(0, MonthIncrement, 0)
		}
	}
	log.Printf("Read a total of %v values from the large series", len(largeSeries))

	log.Println("Slowly read the data from API to the small series")
	smallSeries := make([]Statistikk, 0)
	{
		limiter := time.Tick(500 * time.Millisecond)
		fromDate := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
		toDate := time.Date(2020, 8, 1, 0, 0, 0, 0, time.UTC)
		aDate := fromDate
		const MonthIncrement = 5
		for aDate.Before(toDate) {
			log.Printf("Reading from %v to %v", aDate, aDate.AddDate(0, MonthIncrement, 0))
			tmp, err := Query(aDate, aDate.AddDate(0, MonthIncrement, 0), OldOrg)
			if err != nil {
				return err
			}
			smallSeries = append(smallSeries, tmp...)
			<-limiter
			aDate = aDate.AddDate(0, MonthIncrement, 0)
		}
	}
	log.Printf("Read a total of %v values from the small series", len(largeSeries))

	// Time needs to be in the same timezone since
	collatorMap := make(map[time.Time]Statistikk, 0)
	for _, v := range largeSeries {
		v := v.CalcSum()
		collatorMap[v.Timestamp] = v
		if v.Timestamp.Year() < 1000 {
			fmt.Println("Incorrect data in large series: ", v)
		}
	}

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

	work := SplitStatistikkArrayIntoChunks(collatedSeries, 5000)
	log.Printf("Beginning transmission to BigQuery, splitting workload into %v parts", len(work))
	limiter := time.Tick(2000 * time.Millisecond)
	for i := range work {
		log.Printf("Submitting %v of %v parts, this one has  %v rows", i, len(work), len(work[i]))
		if err := tableRef.Inserter().Put(ctx, work[i]); err != nil {
			return err
		}
		<-limiter
	}

	//
	// Reshape the data and send again to BigQuery
	//

	metrics := make([]Metric, 0)

	for _, v := range collatedSeries {
		metrics = append(metrics, v.ToMetrics()...)
	}
	fmt.Printf("Created %v lines of metrics", len(metrics))

	//
	// Create the table for metrics, delete table if an old one exists.
	//

	var MetricsTableName string = "navmetrics"

	metricSchema, err := bigquery.InferSchema(Metric{})
	if err != nil {
		return
	}
	metricsmetaData := &bigquery.TableMetadata{
		Schema:         metricSchema,
		ExpirationTime: time.Now().AddDate(2, 0, 0), // Table will be automatically deleted in 2 years.
	}
	metricsTableRef := client.Dataset(datasetName).Table(MetricsTableName)

	// Delete the table if it exists.
	_, err = metricsTableRef.Metadata(ctx)
	if err == nil {
		if err := metricsTableRef.Delete(ctx); err != nil {
			return err
		}
	}
	if err := metricsTableRef.Create(ctx, metricsmetaData); err != nil {
		return err
	}

	// Split workload into chungs and send to BigQuery.

	metricsWork := SplitMetricArrayIntoChunks(metrics, 5000)
	log.Printf("Beginning transmission to BigQuery, splitting workload into %v parts", len(metricsWork))
	metricsLimiter := time.Tick(2000 * time.Millisecond)
	for i := range metricsWork {
		log.Printf("Submitting %v of %v metric parts, this one has  %v rows", i, len(metrics), len(metricsWork[i]))
		if err := metricsTableRef.Inserter().Put(ctx, metricsWork[i]); err != nil {
			return err
		}
		<-metricsLimiter
	}
	return err
}

// SplitStatistikkArrayIntoChunks divide buf slice into parts of lim and returns
// an array of slices.
func SplitStatistikkArrayIntoChunks(buf []Statistikk, lim int) [][]Statistikk {
	var chunk []Statistikk
	chunks := make([][]Statistikk, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:]) // :len(buf)
	}
	return chunks
}

// SplitArrayIntoChunks divide buf slice into parts of lim and returns
// an array of slices.
func SplitMetricArrayIntoChunks(buf []Metric, lim int) [][]Metric {
	var chunk []Metric
	chunks := make([][]Metric, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:]) // :len(buf)
	}
	return chunks
}
