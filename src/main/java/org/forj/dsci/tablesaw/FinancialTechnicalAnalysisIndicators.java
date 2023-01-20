package org.forj.dsci.tablesaw;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.numbers.NumberRollingColumn;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.*;
import tech.tablesaw.plotly.components.threeD.Scene;
import tech.tablesaw.plotly.traces.ScatterTrace;
import tech.tablesaw.selection.Selection;
import tech.tablesaw.table.TableSlice;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.forj.dsci.tablesaw.PlotlyUtils.makePage;
import static org.forj.dsci.tablesaw.YahooFinanceApiUtils.getData;

/**
 * originally going to write about all 3/4 tech indicators:  https://colab.research.google.com/drive/1g26txHQUeng8PW7pS8yR0uN3isUfjpwX
 *
 * https://medium.com/p/f16d0538b7c2
 *
 * 1. Stochastic Oscillator: https://colab.research.google.com/drive/1gkydOV2DFwco86_dA4eMlBaZxH-aATRX
 *
 * tablesaw - plotly
 * https://github.com/jtablesaw/tablesaw/tree/master/jsplot/src/test/java/tech/tablesaw/examples
 *
 */
public class FinancialTechnicalAnalysisIndicators
{
    public static void main(String[] args) throws IOException
    {
        //Table bands = getBollingerBands("goog", 1, 14);
        //System.out.println(bands.print());
        //graphBollinger("goog",1, 20);
        graphStochastic("ce",1, 20, 3);
        //graphRSI("goog",1, 14);
        //graphLines();
        //Table prices = getData(Arrays.asList("goog", "msft", "aapl"));

        //System.out.println(combinedHistory.print());
        //Table copyOfOriginalPrices = prices.copy();

        //Table rsiTable = getRSI(prices, 14);
        //System.out.println(rsiTable.print());

        //Table bollingerBandPcts = getBollingerBands(prices, 14);
        //System.out.println(bollingerBandPcts.print());

//        Table stochs = getStochasticOscillators(prices, 14, 3);
//        System.out.println(stochs.printAll());
//        stochs.write().csv("allStochs.csv");



    }

    // todo-15 look for source of indicators online to compare these to

    /**
     * https://www.investopedia.com/terms/r/rsi.asp
     *
     * @param symbols
     * @param years
     * @param period
     * @return
     */
    public static Table getRSI(List<String> symbols,
                               int years,
                               int period) throws IOException
    {

        Table prices = getData(symbols, years);
        symbols.remove("date");

        DateColumn dateColumn = prices.dateColumn("date")
                                      .copy();

        Table rsiTable = Table.create(dateColumn);

        // for each stock symbol:
        //  1. calculate daily returns
        //  2. track 'up gains' and 'down losses' using two arrays
        for (String symbol : symbols) {
            DoubleColumn diff = prices.doubleColumn(symbol)
                                          .difference();
            // todo-15 java streams api to produce two lists?
            List<Double> gains = new ArrayList<>();
            List<Double> losses = new ArrayList<>();
            for (double returns : diff) {
                if (returns < 0) {
                    gains.add(0.0);
                    losses.add(returns);
                } else {
                    gains.add(returns);
                    losses.add(0.0);
                }
            }

            DoubleColumn meanGains = DoubleColumn.create("gains", gains)
                                                 .rolling(period)
                                                 .mean();

            DoubleColumn meanLosses = DoubleColumn.create("losses", losses)
                                                  .abs()
                                                  .rolling(period)
                                                  .mean();

            // rsi = 100 - (100 / (1 + up_sma / down_sma))
            DoubleColumn rsi = meanGains.divide(meanLosses)
                                        .add(1)
                                        .power(-1.0)
                                        .multiply(-100)
                                        .add(100);
            rsi.setName(symbol);
            rsiTable.addColumns(rsi);
        }
        return rsiTable;
    }

    public static Table getBollingerBandPercentages(List<String> symbols,
                                                    int years,
                                                    int period) throws IOException
    {
        // NOTE: does not account for idea that trading dates may not add up to be equal.
        Table prices = getData(symbols);
        symbols.remove("date");

        DateColumn dateColumn = prices.dateColumn("date").copy();

        Table bbpTable = Table.create(dateColumn);

        for (String symbol : symbols) {
            Table bbands = getBollingerBands(symbol, years, period);

            DoubleColumn bbp =
                    getBollingerBandPercentage(prices.doubleColumn(symbol),
                                               bbands.doubleColumn("top"),
                                               bbands.doubleColumn("bottom"));

            bbpTable.addColumns(bbp.setName(symbol));
        }
        return bbpTable;
    }

    public static Table getBollingerBands(String symbol) throws IOException
    {
        return getBollingerBands(symbol, 0, 0);
    }

    public static Table getBollingerBands(String symbol, int years, int period) throws IOException
    {
        if(period == 0) {
            period = 20;
        }
        if(years == 0) {
            years = 1;
        }
        Table prices = getData(Arrays.asList(symbol), years);
        return getBollingerBands(prices.dateColumn("date"),
                                 prices.doubleColumn(symbol).setName("adj_close"),
                                 period,
                                 2);
    }

    public static Table getBollingerBands(DateColumn dates,
                                          DoubleColumn prices,
                                          int period,
                                          int numStdDevs)
    {
        if(numStdDevs == 0) {
            numStdDevs = 2;
        }
        DoubleColumn sma = prices.rolling(period).mean();
        DoubleColumn std = prices.rolling(period).stdDev();

        Table bbTable = Table.create(dates, prices);

        bbTable.addColumns(sma.add(std.multiply(numStdDevs)).setName("top"),
                           sma.subtract(std.multiply(numStdDevs)).setName("bottom"),
                           sma.setName("sma"));

        return bbTable;
    }

    public static DoubleColumn getBollingerBandPercentage(DoubleColumn prices,
                                                          DoubleColumn top,
                                                          DoubleColumn bottom)
    {
        return (prices.subtract(bottom)).divide(top.subtract(bottom));
    }

    // todo - not finished
    //
    public static Table getStochasticOscillators(String symbol, int years,
                                                 int k, int d) throws IOException
    {
        Table prices = getData(Arrays.asList(symbol), years);

        Table stochTable = Table.create(prices.dateColumn("date").copy());
        StringColumn symCol =
                StringColumn.create("symbol")
                            .addAll(Collections.nCopies(prices.rowCount(), symbol));

        NumberRollingColumn kRoll = prices.doubleColumn(symbol).rolling(k);
        DoubleColumn kMax = kRoll.max();
        DoubleColumn kMin = kRoll.min();
        DoubleColumn stochsK = DoubleColumn.create("percent_k");

        for (int i = 0; i < prices.rowCount(); i++) {
            if(i < k - 1) {
                stochsK.appendMissing();
            }
            else {
                double todayClose = (double)prices.get(i, 1);
                double closeMinusLow = todayClose - kMin.get(i);
                double highMinusLow = kMax.get(i) - kMin.get(i);
                stochsK.append((closeMinusLow/highMinusLow) * 100);
            }
        }

        DoubleColumn stochsD = stochsK.rolling(d).mean().setName("percent_d");

        stochTable.addColumns(symCol,
                              stochsK,
                              stochsD,
                              prices.doubleColumn(symbol).setName("prices"));
        return stochTable;
    }

    public static Table getStochasticOscillators(List<String> symbols, int years, int k, int d) throws IOException
    {
        Table prices = getData(symbols, years);

        Table allStochs = null;
        Table[] listOfStochs = new Table[symbols.size()];
        // for each stock symbol:
        for (String symbol : symbols) {
            Table stochTable = Table.create(prices.dateColumn("date").copy());
            StringColumn symCol = StringColumn.create("symbol").addAll(Collections.nCopies(prices.rowCount(), symbol));
            NumberRollingColumn kRoll = prices.doubleColumn(symbol).rolling(k);
            DoubleColumn kMax = kRoll.max();
            DoubleColumn kMin = kRoll.min();
            DoubleColumn stochsK = DoubleColumn.create("percent_k");
            for (int i = 0; i < prices.rowCount(); i++) {
                if(i < k - 1) {
                    stochsK.appendMissing();
                }
                else {
                    double todayClose = (double)prices.get(i, symbols.indexOf(symbol) + 1);
                    double closeMinusLow = todayClose - kMin.get(i);
                    double highMinusLow = kMax.get(i) - kMin.get(i);
                    stochsK.append((closeMinusLow/highMinusLow) * 100);
                }
            }
            DoubleColumn stochsD = stochsK.rolling(d).mean().setName("percent_d");
            stochTable.addColumns(symCol, stochsK, stochsD, prices.doubleColumn(symbol).setName("prices"));
            listOfStochs[symbols.indexOf(symbol)] = stochTable;
        }
        allStochs = listOfStochs[0];

        if(listOfStochs.length > 1) {
            for (int x = 1; x < listOfStochs.length; x++) {
                allStochs = allStochs.append(listOfStochs[x]);
            }
        }

        return allStochs;
    }

    // todo-15 graph indicator value(s) as lines
    public static void graphBollinger(String symbol, int years, int period) throws IOException
    {
        Table bands = getBollingerBands(symbol, years, period);
        DoubleColumn top = bands.doubleColumn("top");
        DoubleColumn bottom = bands.doubleColumn("bottom");
        DoubleColumn bbp = getBollingerBandPercentage(bands.doubleColumn("adj_close"),
                                                      top,
                                                      bottom);
        //bands.addColumns(bbp.setName("bbp"));

        DateColumn x = bands.dateColumn("date").copy();
        bands.removeColumns("date");

        //DoubleColumn bbpAsPrice = bottom.add(top.min(bottom).multiply(bbp));
        //bands.addColumns(bbpAsPrice.setName("bbp_price"));

        ScatterTrace[] traces =
                bands.columnNames().stream()
                       .map(col -> ScatterTrace.builder(x, bands.nCol(col)).mode(ScatterTrace.Mode.LINE).build())
                       .collect(Collectors.toList()).toArray(new ScatterTrace[bands.columnCount()]);


        Layout layout = Layout.builder()
                              .title("Bollinger Bands for " + symbol)
                              .build();

        Plot.show(new Figure(layout, traces));
    }

    // todo-15 add 3 or more of the above and below as gists (or confirm they already are gists)

    // todo-15 plot horizontal lines (solid or dashed)
    //   * one example complete (rsi)

    // todo-15 2 or more charts in one plot
    //

    // todo-15 graph price and indicator together for a stock
    //  bollinger (DONE)
    //  stochastic (DONE)
    //  rsi

    // todo-15 graph above as stacked charts with bottom chart showing
    //  bbp, rsi, stochastic and top chart showing price

    public static void graphRSI(String symbol,
                                int years,
                                int period) throws IOException
    {
        Table rsi = getRSI(Arrays.asList(symbol), years, period);
        DateColumn x = rsi.dateColumn("date").copy();

        ScatterTrace rsiTrace = ScatterTrace.builder(x, rsi.nCol(symbol))
                                              .mode(ScatterTrace.Mode.LINE)
                                              .build();

//        double[] line70 = new double[rsi.rowCount()];
//        double[] line30 = new double[rsi.rowCount()];
//        Arrays.fill(line70,70.0);
//        Arrays.fill(line30,30.0);


        Double[] line70 = IntStream.range(0, rsi.rowCount()).mapToObj(i -> (i % 5 == 0) ? 70.0 : null)
                                   .collect(Collectors.toList()).toArray(new Double[rsi.rowCount()]);
        Double[] line30 = IntStream.range(0, rsi.rowCount()).mapToObj(i -> (i % 5 == 0) ? 30.0 : null)
                                   .collect(Collectors.toList()).toArray(new Double[rsi.rowCount()]);
        DoubleColumn.create("70", line70);

        ScatterTrace line70Trace = ScatterTrace.builder(x, DoubleColumn.create("80", line70))
                                               .mode(ScatterTrace.Mode.MARKERS).marker(Marker.builder().symbol(Symbol.CIRCLE).size(3).build())
                                               .build();

        ScatterTrace line30Trace = ScatterTrace.builder(x, DoubleColumn.create("30", line30))
                                               .mode(ScatterTrace.Mode.MARKERS).marker(Marker.builder().symbol(Symbol.CIRCLE).size(3).build())
                                               .build();

        Layout layout = Layout.builder()
                              .title("RSI for '" + symbol.toUpperCase() + "'")
                              //  .width(1200)
                              // .height(900)
                              .build();
        Figure figure = Figure.builder()
                              //   .layout(layout).addTraces(traces)
                              .layout(layout).addTraces(rsiTrace, line70Trace, line30Trace).build();
        Plot.show(figure);
    }

    // todo - should be "paper_bgcolor" in output.  https://stackoverflow.com/questions/50853447/change-plot-background-color
    //                                   .plotBgColor("#000")
    //                                   .paperBgColor("#000")

    public static void graphStochastic(String symbol,
                                       int years,
                                       int kPeriod,
                                       int dPeriod) throws IOException
    {
        Table stochs = getStochasticOscillators(symbol, years, kPeriod, dPeriod);

        DateColumn x = stochs.dateColumn("date").copy();

        ScatterTrace kTrace = ScatterTrace.builder(x, stochs.nCol("percent_k"))
                                          .mode(ScatterTrace.Mode.LINE)
                                          .line(Line.builder().color("red").build())
                                          .name("%k")
                                          .xAxis("x1")
                                          .yAxis("y2")
                                          .build();

        ScatterTrace dTrace = ScatterTrace.builder(x, stochs.nCol("percent_d"))
                                          .mode(ScatterTrace.Mode.LINE)
                                          .line(Line.builder().color("orange").build())
                                          .name("%d")
                                          .xAxis("x1")
                                          .yAxis("y2")
                                          .build();

        ScatterTrace priceTrace = ScatterTrace.builder(x, stochs.nCol("prices"))
                                              .mode(ScatterTrace.Mode.LINE)
                                              .name("price")
                                              .xAxis("x1")
                                              .yAxis("y1")
                                              .build();

        Grid grid = Grid.builder()
                        .columns(1)
                        .rows(2)
                        .pattern(Grid.Pattern.COUPLED)
                        .build();

        Layout stochLayout = Layout.builder()
                                   .height(600)
                                   .width(1200)
                                   .showLegend(true)
                                   .title("Price & Stochastics (%k & %d) for '" + symbol.toUpperCase() + "'")
                                   .yAxis(Axis.builder().title("Price").zeroLineColor("white").showZeroLine(false).build())
                                   .yAxis2(Axis.builder().range(-40.0f, 140.0f).autoRange(Axis.AutoRange.FALSE)
                                               .title("Stochastics %").zeroLineColor("white").showZeroLine(false).build())
                                   .xAxis(Axis.builder().title("Date").showZeroLine(false).build())
                                   .margin(Margin.builder().build())
                                   .grid(grid)
                                   .build();

        Figure stochFigure = Figure.builder().layout(stochLayout).addTraces(priceTrace, kTrace, dTrace).build();

        Plot.show(stochFigure);
    }


    /*        Layout priceLayout = Layout.builder()
                            //       .title("Prices for '" + symbol.toUpperCase() + "'")
                                   .width(1200)
                                   .xAxis(Axis.builder().visible(false).build())

                                   .margin(Margin.builder().bottom(1).build())
                                   .title("Prices & Stochastics for '" + symbol.toUpperCase() + "'")
                                   .build();*/

//        Figure priceFigure = Figure.builder()
//                                   .layout(priceLayout).addTraces(priceTrace).build();



    // 7. Create an HTML page as a string
    // String divName1 = "plot1";
    // String divName2 = "plot2";
    // String page = makePage(priceFigure, stochFigure, divName1, divName2);


//        File outputFile = Paths.get("multiplot.html").toFile();
//        try {
//            try (FileWriter fileWriter = new FileWriter(outputFile)) {
//                fileWriter.write(page);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    //Plot.show(page, new File("temp_delete_me.html"));




    public static void graphLines() throws IOException
    {
        // https://github.com/jtablesaw/tablesaw/blob/master/jsplot/src/test/java/tech/tablesaw/examples/LinePlotExample.java

        List<String> symbols = Arrays.asList("goog", "msft", "aapl");
        Table prices = getData(symbols);

        DateColumn x = prices.dateColumn("date");

        List<NumericColumn<?>> priceCols =
                symbols.stream().map(sym -> prices.nCol(sym)).collect(Collectors.toList());
        ScatterTrace[] traces =
                symbols.stream()
                       .map(sym -> prices.nCol(sym))
                       .map(col -> ScatterTrace.builder(x, col).mode(ScatterTrace.Mode.LINE).build())
                       .collect(Collectors.toList()).toArray(new ScatterTrace[priceCols.size()]);


        Layout layout = Layout.builder()
                              .title("Stock Adj. Close for " + String.join(", ", symbols))
                              .width(900)
                              .height(1200)
                              .build();

        Plot.show(new Figure(layout, traces));
    }

    public static void multiPlot(Table table1, Table table2)
    {
        // 8. Write the string to a file
        // uncomment to try (see imports also)

    /*
        File outputFile = Paths.get("multiplot.html").toFile();
        try {
          try (FileWriter fileWriter = new FileWriter(outputFile)) {
            fileWriter.write(page);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
    */

        // 9. Open the default desktop Web browser on the file so you can see it
        // uncomment to try

        // new Browser().browse(outputFile);
    }

}

//
//        if print_bands:
//        plt.plot(prices, label="Stock Values", color="black")
//        plt.plot(top_band, label="Top Band", color="red")
//        plt.plot(bottom_band, label="Bottom Band", color="green")
//        plt.xlabel("Days")
//        plt.ylabel("Price")
//        plt.legend()
//        plt.show()
//
//        return bottom_band, top_band
//        }