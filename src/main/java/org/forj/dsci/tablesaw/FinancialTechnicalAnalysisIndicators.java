package org.forj.dsci.tablesaw;

import org.apache.commons.lang3.ArrayUtils;
import smile.math.matrix.DenseMatrix;
import smile.math.matrix.Matrix;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.numbers.NumberRollingColumn;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

import static org.forj.dsci.tablesaw.YahooFinanceApiUtils.getData;

public class FinancialTechnicalAnalysisIndicators
{
    public static void main(String[] args) throws IOException
    {

        Table prices = getData(Arrays.asList("goog", "msft", "aapl"));
        //System.out.println(combinedHistory.print());
        Table copyOfOriginalPrices = prices.copy();

        //Table rsiTable = getRSI(prices, 14);
        //System.out.println(rsiTable.print());

        //Table bollingerBandPcts = getBollingerBands(prices, 14);
        //System.out.println(bollingerBandPcts.print());

        Table stochs = getStochasticOscillators(prices, 14, 3);
        System.out.println(stochs.printAll());
        stochs.write().csv("allStochs.csv");



    }



    public static Table getRSI(Table prices, int period) throws IOException
    {
        Table copyPrices = prices.copy();
        List<String> symbols = prices.columnNames();
        symbols.remove("date");

        DateColumn dateColumn = prices.dateColumn("date").copy();

        // create an m x n matrix initialized to 0, where m is the number of rows,
        // and n is the number of stocks we are following.
        Table rsiTable = Table.create(dateColumn);

        // for each stock symbol:
        //  1. calculate daily returns
        //  2. track 'up gains' and 'down losses' using two arrays
        for (String symbol : symbols) {
            DoubleColumn diff = copyPrices.doubleColumn(symbol).difference();
            List<Double> gains = new ArrayList<>();
            List<Double> losses = new ArrayList<>();
            for (Iterator<Double> it = diff.iterator(); it.hasNext(); ) {
                double returns = it.next();
                if (returns < 0) {
                    gains.add(0.0);
                    losses.add(returns);
                } else {
                    gains.add(returns);
                    losses.add(0.0);
                }
            }
            DoubleColumn gainsCol = DoubleColumn.create("gains", gains);
            DoubleColumn lossesCol = DoubleColumn.create("losses", losses).abs();

            DoubleColumn meanGains = gainsCol.rolling(period).mean();
            DoubleColumn meanLosses = lossesCol.rolling(period).mean();

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

    public static Table getBollingerBands(Table prices, int period)
    {
        Table copyPrices = prices.copy();
        List<String> symbols = prices.columnNames();
        symbols.remove("date");

        DateColumn dateColumn = prices.dateColumn("date").copy();

        Table bbpTable = Table.create(dateColumn);

        // for each stock symbol:
        //  1. calculate daily returns
        //  2. track 'up gains' and 'down losses' using two arrays
        for (String symbol : symbols) {
            DoubleColumn sma = copyPrices.doubleColumn(symbol).rolling(period).mean();
            double std = sma.standardDeviation();
            DoubleColumn topBand = sma.add(2 * std);
            DoubleColumn bottomBand = sma.subtract(2 * std);
            // bbp
            DoubleColumn bbp = copyPrices.doubleColumn(symbol).subtract(bottomBand).divide(topBand.subtract(bottomBand));
            bbpTable.addColumns(bbp.setName(symbol));
        }
        return bbpTable;
    }

    public static Table getStochasticOscillators(Table prices, int k, int d)
    {
        Table copyPrices = prices.copy();
        List<String> symbols = prices.columnNames();
        symbols.remove("date");

        DateColumn dateColumn = prices.dateColumn("date").copy();

        Table allStochs = null;
        Table[] listOfStochs = new Table[symbols.size()];
        // for each stock symbol:
        for (String symbol : symbols) {
            Table stochTable = Table.create(dateColumn.copy());
            StringColumn symCol = StringColumn.create("symbol").addAll(Collections.nCopies(copyPrices.rowCount(), symbol));
            System.out.println(symCol.size());
            NumberRollingColumn kRoll = copyPrices.doubleColumn(symbol).rolling(k);
            DoubleColumn kMax = kRoll.max();
            DoubleColumn kMin = kRoll.min();
            DoubleColumn stochsK = DoubleColumn.create("percent_k");
            stochsK.setMissingTo(0.0);
            for (int i = 0; i < prices.rowCount(); i++) {
                if(i < k - 1) {
                    stochsK.append(0);
                }
                else {
                    double todayClose = (double)copyPrices.get(i, symbols.indexOf(symbol) + 1);
                    double closeMinusLow = todayClose - kMin.get(i);
                    double highMinusLow = kMax.get(i) - kMin.get(i);
                    stochsK.append((closeMinusLow/highMinusLow) * 100);
                }
            }
            DoubleColumn stochsD = stochsK.rolling(d).mean().setName("percent_d");
            stochsD.setMissingTo(0.0);
            stochTable.addColumns(symCol, stochsK, stochsD);
            System.out.println(stochTable.printAll());
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