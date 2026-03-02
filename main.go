package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/encratite/commons"
	"github.com/encratite/yahoo"
)

var configuration *Configuration

type Configuration struct {
	YahooDirectory string `yaml:"yahooDirectory"`
	ComponentsPath string `yaml:"componentsPath"`
	StartDate commons.SerializableDate `yaml:"startDate"`
}

func main() {
	configuration = commons.LoadConfiguration[Configuration]("yaml/mahwah.yaml")
	download := flag.Bool("download", false, "Download S&P 500 components from Yahoo Finance")
	flag.Parse()
	if *download {
		downloadComponents()
	} else {
		flag.Usage()
	}
}

func downloadComponents() {
	columns := []string{
		"date",
		"tickers",
	}
	symbols := map[string]struct{}{}
	commons.ReadCSVColumns(configuration.ComponentsPath, columns, func (records []string) {
		date := commons.MustParseTime(records[0])
		if date.Before(configuration.StartDate.Time) {
			return
		}
		iter := strings.SplitSeq(records[1], ",")
		for symbol := range iter {
			symbols[symbol] = struct{}{}
		}
	})
	log.Printf("Detected %d components\n", len(symbols))
	startTime := configuration.StartDate
	endTime := time.Now().UTC()
	period1 := startTime.Unix()
	period2 := endTime.Unix()
	for symbol := range symbols {
		if strings.Contains(symbol, `/\`) {
			log.Printf("Invalid symbol name: %s", symbol)
			continue
		}
		fileName := fmt.Sprintf("%s.csv", symbol)
		path := filepath.Join(configuration.YahooDirectory, fileName)
		if commons.FileExists(path) {
			continue
		}
		data, err := yahoo.GetFinanceData(symbol, period1, period2, yahoo.Interval1D, false)
		if err != nil {
			log.Printf("Failed to download data for symbol %s: %v", symbol, err)
			continue
		}
		if len(data.Chart.Result) != 1 {
			log.Printf("Invalid number of chart results for symbol %s", symbol)
			continue
		}
		result := data.Chart.Result[0]
		timestamps := result.Timestamp
		quotes := result.Indicators.Quote[0]
		lengthValues := []int{
			len(quotes.Open),
			len(quotes.High),
			len(quotes.Low),
			len(quotes.Close),
			len(quotes.Volume),
		}
		mismatch := false
		for _, length := range lengthValues {
			if len(timestamps) != length {
				log.Printf("Mismatch between number of timestamps and quote value for symbol %s", symbol)
				mismatch = true
				break
			}
		}
		if mismatch {
			continue
		}
		csvBuilder := strings.Builder{}
		csvBuilder.WriteString("date,open,high,low,close,volume\n")
		for i, timestamp := range timestamps {
			open := quotes.Open[i]
			high := quotes.High[i]
			low := quotes.Low[i]
			close := quotes.Close[i]
			volume := int(quotes.Volume[i])
			timestamp := time.Unix(timestamp, 0).UTC()
			date := commons.GetDate(timestamp)
			dateString := commons.GetDateString(date)
			line := fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%d\n", dateString, open, high, low, close, volume)
			csvBuilder.WriteString(line)
		}
		commons.WriteFileString(path, csvBuilder.String())
		log.Printf("Wrote %s", path)
	}
}