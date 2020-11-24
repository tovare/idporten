package main

import (
	"fmt"

	idharvest "github.com/tovare/idporten"
)

func main() {
	fmt.Println("hello")
	err := idharvest.SendEverythingToBigquery()
	if err != nil {
		fmt.Println(err)
	}

	/*
		a, _ := readSeries("https://statistikk-utdata.difi.no/991825827/idporten-innlogging/hours/sum/months?from=2013-05-01T00:00:00Z&to=2019-05-31T23:59:59Z&categories=TE-orgnum=889640782")
		for i := range a {
			fmt.Println(a[i].Timestamp)
		}
	*/
}

/*func readSeries(query string) (Statistikk, error) {

	var sumresult Statistikk

	res, err := http.Get(query)
	if err != nil {
		return sumresult, err
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return sumresult, err
	}
	json.Unmarshal(body, &sumresult)

	for _, v := range sumresult {
		fmt.Println(v.Timestamp)
	}

	return sumresult, nil
}
*/
