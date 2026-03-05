package main

import (
	"cmp"
	"flag"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/encratite/commons"
	"github.com/encratite/yahoo"
	"github.com/encratite/ohlc"
)

const (
	scanLimit = 100
	monthsPerYear = 12
)

var configuration *Configuration

type rebalanceMode int

const (
	rebalanceStartOfMonth rebalanceMode = iota
	rebalanceEndOfMonth
	rebalanceFirstWeekday
)

type Configuration struct {
	BarchartDirectory string `yaml:"barchartDirectory"`
	YahooDirectory string `yaml:"yahooDirectory"`
	ComponentsPath string `yaml:"componentsPath"`
	HistoryStartDate commons.SerializableDate `yaml:"historyStartDate"`
	StartDate commons.SerializableDate `yaml:"startDate"`
	EndDate commons.SerializableDate `yaml:"endDate"`
	InitialCash float64 `yaml:"initialCash"`
	ExecutionCost float64 `yaml:"executionCost"`
	BorrowCost float64 `yaml:"borrowCost"`
	RiskFreeRate float64 `yaml:"riskFreeRate"`
	Tax float64 `yaml:"tax"`
	RebalanceMode string `yaml:"rebalanceMode"`
	RebalanceDay commons.SerializableWeekday `yaml:"rebalanceDay"`
	LookbackPeriod int `yaml:"lookbackPeriod"`
	LongPositions int `yaml:"longPositions"`
	ShortPositions int `yaml:"shortPositions"`
}

type PlotData struct {
	Dates []string `json:"dates"`
	Returns []float64 `json:"returns"`
}

type componentMap map[time.Time][]string

type assetData struct {
	symbol string
	records []dateRecord
	recordMap map[time.Time]indexRecord
	currentClose float64
	sharpeRatio float64
}

type dateRecord struct {
	date time.Time
	close float64
}

type indexRecord struct {
	date time.Time
	index int
	close float64
}

type stockPosition struct {
	symbol string
	previousClose float64
	size float64
	short bool
	asset *assetData
}

func main() {
	configuration = commons.LoadConfiguration[Configuration]("yaml/mahwah.yaml")
	download := flag.Bool("download", false, "Download S&P 500 components from Yahoo Finance")
	backtest := flag.Bool("backtest", false, "Perform backtest")
	flag.Parse()
	if *download {
		downloadComponents()
	} else if *backtest {
		performBacktest()
	} else {
		flag.Usage()
	}
}

func downloadComponents() {
	components := loadComponents()
	symbols := map[string]struct{}{}
	for _, value := range components {
		for _, symbol := range value {
			symbols[symbol] = struct{}{}
		}
	}
	log.Printf("Detected %d components\n", len(symbols))
	startTime := configuration.HistoryStartDate
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

func performBacktest() {
	fmt.Printf("Loading components\n")
	components := loadComponents()
	fmt.Printf("Loading assets\n")
	assets := loadAssets()
	fmt.Printf("Performing backtest\n")
	startDate := commons.GetDate(configuration.StartDate.Time)
	endDate := commons.GetDate(configuration.EndDate.Time)
	counter := 0
	var componentSymbols []string
	for date := configuration.StartDate.Time; counter < scanLimit; date = date.AddDate(0, 0, -1) {
		latest, exists := components[date]
		if exists {
			componentSymbols = latest
			break
		}
		counter++
	}
	if componentSymbols == nil {
		commons.Fatalf("Unable to determine latest index components")
	}
	cash := configuration.InitialCash
	capitalGains := 0.0
	returns := []float64{}
	plotData := PlotData{
		Dates: []string{},
		Returns: []float64{},
	}
	positions := []stockPosition{}
	closePositions := func (date time.Time) {
		if len(positions) == 0 {
			return
		}
		previousCash := cash
		for _, position := range positions {
			var currentClose, previousClose float64
			borrowCost := 0.0
			if position.short {
				currentClose = position.previousClose
				previousClose = position.asset.currentClose
				borrowCost = configuration.BorrowCost / monthsPerYear
			} else {
				currentClose = position.asset.currentClose
				previousClose = position.previousClose
			}
			pnl := position.size * (getRateOfChange(currentClose, previousClose) - configuration.ExecutionCost - borrowCost)
			if math.IsNaN(pnl) || math.IsInf(pnl, 1) || math.IsInf(pnl, -1) {
				fmt.Printf("symbol = %s, position.size = %.2f, currentClose = %.2f, previousClose = %.2f\n", position.asset.symbol, position.size, currentClose, previousClose)
				commons.Fatalf("Invalid returns: %f", pnl)
			}
			cash += pnl
			capitalGains += pnl
		}
		r := getRateOfChange(cash, previousCash)
		dateString := commons.GetDateString(date)
		totalReturn := getRateOfChange(cash, configuration.InitialCash)
		plotData.Dates = append(plotData.Dates, dateString)
		plotData.Returns = append(plotData.Returns, totalReturn)
		returns = append(returns, r)
		positions = []stockPosition{}
		if date.Month() == time.January {
			if capitalGains > 0 {
				cash -= configuration.Tax * capitalGains
				capitalGains = 0.0
			}
		}
	}
	rebalanceMode := getRebalanceMode(configuration.RebalanceMode)
	previousRebalance := false
	var rebalanceDate time.Time
	endOfMonth := getEndOfMonthDates()
	for date := startDate; date.Before(endDate); date = date.AddDate(0, 0, 1) {
		weekday := date.Weekday()
		if weekday == time.Saturday || weekday == time.Sunday {
			continue
		}
		if rebalanceMode == rebalanceFirstWeekday && weekday != configuration.RebalanceDay.Weekday {
			// fmt.Printf("Skipping date = %s, weekday = %s, rebalanceDay = %s\n", commons.GetDateString(date), weekday, configuration.RebalanceDay.Weekday)
			continue
		}
		var performRebalance bool
		switch rebalanceMode {
		case rebalanceStartOfMonth, rebalanceFirstWeekday:
			performRebalance = !previousRebalance || rebalanceDate.Month() != date.Month()
		case rebalanceEndOfMonth:
			_, isEndOfMonth := endOfMonth[date]
			performRebalance = !previousRebalance || isEndOfMonth
		default:
			commons.Fatalf("Unknown rebalance mode: %d", rebalanceMode)
		}
		if !performRebalance {
			// fmt.Printf("%s Skipping, rebalanceDate = %s, previousRebalance = %t\n", commons.GetDateString(date), commons.GetDateString(rebalanceDate), previousRebalance)
			continue
		}
		latestComponents, exists := components[date]
		if exists {
			componentSymbols = latestComponents
		}
		enabledAssets := []*assetData{}
		for _, symbol := range componentSymbols {
			asset, exists := commons.FindPointer(assets, func (a assetData) bool {
				return a.symbol == symbol
			})
			if !exists {
				continue
			}
			asset.sharpeRatio = math.NaN()
			record, exists := asset.recordMap[date]
			if !exists {
				continue
			}
			asset.currentClose = record.close
			if math.IsNaN(asset.currentClose) {
				commons.Fatalf("NaN close in %s at %s", symbol, commons.GetDateString(date))
			}
			records := []dateRecord{}
			lookbackIndex := max(record.index - configuration.LookbackPeriod, 0)
			for i := lookbackIndex; i <= record.index; i++ {
				record := asset.records[i]
				if len(records) > 0 {
					lastRecord := records[len(records) - 1]
					if record.date.Month() != lastRecord.date.Month() {
						records = append(records, record)
					}
				} else {
					records = append(records, record)
				}
			}
			returns := []float64{}
			for i := 1; i < len(records); i++ {
				record := records[i]
				previousRecord := records[i - 1]
				r := getRateOfChange(record.close, previousRecord.close)
				returns = append(returns, r)
			}
			meanReturn := commons.Mean(returns)
			volatility := commons.StdDev(returns)
			asset.sharpeRatio = meanReturn / volatility
			enabledAssets = append(enabledAssets, asset)
		}
		slices.SortFunc(enabledAssets, func (a, b *assetData) int {
			return cmp.Compare(b.sharpeRatio, a.sharpeRatio)
		})
		totalPositions := configuration.LongPositions + configuration.ShortPositions
		if len(enabledAssets) < totalPositions {
			// fmt.Printf("Warning: not enough enabled assets on %s\n", commons.GetDateString(date))
			continue
		}
		closePositions(date)
		fmt.Printf("%s Rebalancing, cash = %s\n", commons.GetDateString(date), commons.FormatMoney(cash))
		size := 1.0 / float64(totalPositions) * cash
		for i := range configuration.LongPositions {
			asset := enabledAssets[i]
			position := stockPosition{
				symbol: asset.symbol,
				previousClose: asset.currentClose,
				size: size,
				short: false,
				asset: asset,
			}
			fmt.Printf("%s symbol = %s, short = false, size = %.2f, currentClose = %.2f, sharpeRatio = %.3f\n", commons.GetDateString(date), asset.symbol, size, asset.currentClose, asset.sharpeRatio)
			positions = append(positions, position)
		}
		for i := range configuration.ShortPositions {
			finalIndex := len(enabledAssets) - 1
			asset := enabledAssets[finalIndex - i]
			position := stockPosition{
				symbol: asset.symbol,
				previousClose: asset.currentClose,
				size: size,
				short: true,
				asset: asset,
			}
			fmt.Printf("%s symbol = %s, short = true, size = %.2f, currentClose = %.2f, sharpeRatio = %.3f\n", commons.GetDateString(date), asset.symbol, size, asset.currentClose, asset.sharpeRatio)
			positions = append(positions, position)
		}
		previousRebalance = true
		rebalanceDate = date
	}
	for i := range assets {
		asset := &assets[i]
		asset.currentClose = asset.getClose(endDate)
	}
	closePositions(endDate)
	totalReturn := getRateOfChange(cash, configuration.InitialCash)
	meanReturn := commons.Mean(returns)
	volatility := commons.StdDev(returns)
	riskFreeRate := configuration.RiskFreeRate / monthsPerYear
	sharpeRatio := math.Sqrt(monthsPerYear) * (meanReturn - riskFreeRate) / volatility
	fmt.Printf("Cash: %s\n", commons.FormatMoney(cash))
	fmt.Printf("Return: %s\n", commons.FormatPercentage(totalReturn, 1.0))
	fmt.Printf("Sharpe ratio: %.2f\n", sharpeRatio)
	arguments := []string{
		"python/plot.py",
	}
	commons.PythonPipe(arguments, plotData)
}

func loadComponents() componentMap {
	columns := []string{
		"date",
		"tickers",
	}
	components := componentMap{}
	commons.ReadCSVColumns(configuration.ComponentsPath, columns, func (records []string) {
		date := commons.MustParseTime(records[0])
		if date.Before(configuration.HistoryStartDate.Time) {
			return
		}
		symbols := strings.Split(records[1], ",")
		components[date] = symbols
	})
	return components
}

func loadAssets() []assetData {
	files := commons.GetFiles(configuration.YahooDirectory, ".csv")
	pattern := regexp.MustCompile("^[A-Z]+")
	columns := []string{
		"date",
		"open",
		"high",
		"low",
		"close",
	}
	assets := []assetData{}
	for _, file := range files {
		base := filepath.Base(file)
		match := pattern.FindStringSubmatch(base)
		if match ==  nil {
			commons.Fatalf("Unable to parse file name: %s", base)
		}
		symbol := match[0]
		records := []dateRecord{}
		recordMap := map[time.Time]indexRecord{}
		index := 0
		previousClose := math.NaN()
		commons.ReadCSVColumns(file, columns, func (values []string) {
			date := commons.MustParseTime(values[0])
			close := commons.MustParseFloat(values[4])
			if close <= 0.0 {
				close = previousClose
			}
			record1 := dateRecord{
				date: date,
				close: close,
			}
			records = append(records, record1)
			record2 := indexRecord{
				close: close,
				index: index,
			}
			recordMap[date] = record2
			index++
			previousClose = close
		})
		asset := assetData{
			symbol: symbol,
			records: records,
			recordMap: recordMap,
			currentClose: math.NaN(),
			sharpeRatio: math.NaN(),
		}
		assets = append(assets, asset)
	}
	return assets
}

func getRateOfChange(a, b float64) float64 {
	return a / b - 1.0
}

func getRebalanceMode(modeString string) rebalanceMode {
	switch modeString {
	case "startOfMonth":
		return rebalanceStartOfMonth
	case "endOfMonth":
		return rebalanceEndOfMonth
	case "firstWeekday":
		return rebalanceFirstWeekday
	}
	commons.Fatalf("Invalid rebalance mode string: %s", modeString)
	return 0
}

func (a *assetData) getClose(date time.Time) float64 {
	d := date
	for range scanLimit {
		record, exists := a.recordMap[d]
		if exists {
			return record.close
		}
		d = d.AddDate(0, 0, -1)
	}
	// fmt.Printf("Failed to find close for %s at %s\n", a.symbol, commons.GetTimeString(date))
	return math.NaN()
}

func getEndOfMonthDates() map[time.Time]struct{} {
	records := ohlc.MustReadBarchart("SPY", configuration.BarchartDirectory, ohlc.TimeFrameD1)
	dates := map[time.Time]struct{}{}
	for i := 0; i < len(records) - 1; i++ {
		record := records[i]
		nextRecord := records[i + 1]
		if record.Timestamp.Month() != nextRecord.Timestamp.Month() {
			dates[record.Timestamp] = struct{}{}
		}
	}
	return dates
}