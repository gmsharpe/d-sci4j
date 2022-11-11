package org.forj.dsci.tablesaw;

import static smile.math.MathEx.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import org.apache.commons.io.input.DemuxInputStream;
import org.apache.commons.lang3.ArrayUtils;
import smile.data.DataFrame;
import smile.data.type.DataType;
import smile.data.type.DataTypes;
import smile.data.vector.Vector;
import smile.io.CSV;
import smile.math.matrix.DenseMatrix;
import smile.math.matrix.JMatrix;
import smile.math.matrix.Matrix;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.plotly.traces.HistogramTrace;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import static org.forj.dsci.tablesaw.PortfolioUtils.*;
import static org.forj.dsci.tablesaw.YahooFinanceApiUtils.*;


public class YahooFinanceApiExample
{
    public static void main(String[] args) throws IOException
    {
        Table combinedHistory = getData(Arrays.asList("goog", "msft", "aapl"));
        //System.out.println(combinedHistory.print());
        Table copyOfOriginalPrices = combinedHistory.copy();

        Table normalized = normalizePrices(combinedHistory);
        //System.out.println(normalized);

        Table portfolioValues = getPortfolioValue(normalized, new double[]{0.1, 0.4, 0.5});
        //System.out.println(portfolioValues.print());

        Table dailyReturns = getDailyReturns(portfolioValues);
        System.out.println("Daily Returns: " + dailyReturns.print());

        double avgDailyReturns = getAverageDailyReturns(dailyReturns);
        System.out.println("Average Daily Returns: " + avgDailyReturns);

        double cumulativeReturn = getCumulativeReturn(portfolioValues);
        System.out.println("Cumulative Returns:  " + cumulativeReturn);

        double sharpe = sharpe(copyOfOriginalPrices, new double[]{0.1, 0.4, 0.5});
        System.out.println("Sharpe Ratio: " + sharpe);


    }

    static Table getOne() throws IOException
    {
        Calendar from = Calendar.getInstance();
        Calendar to = Calendar.getInstance();
        from.add(Calendar.YEAR, -1); // from 1 years ago

        Stock google = YahooFinance.get("GOOG", from, to, Interval.DAILY);
        List<HistoricalQuote> history = google.getHistory();
        history.stream().forEach(System.out::println);

        List<BigDecimal> adjClose = new ArrayList<>();
        List<BigDecimal> close = new ArrayList<>();
        List<BigDecimal> open = new ArrayList<>();
        List<LocalDate> date = new ArrayList<>();
        List<String> symbol = new ArrayList<>();
        List<Long> volume = new ArrayList<>();
        List<BigDecimal> high = new ArrayList<>();
        List<BigDecimal> low = new ArrayList<>();

        history.stream().forEach( q -> {
            adjClose.add(q.getAdjClose());
            close.add(q.getClose());
            open.add(q.getOpen());
            date.add(LocalDateTime.ofInstant(q.getDate().toInstant(), q.getDate().getTimeZone().toZoneId()).toLocalDate());
            symbol.add(q.getSymbol());
            volume.add(q.getVolume());
            high.add(q.getHigh());
            low.add(q.getLow());
        });

        List<Column<?>> cols = Arrays.asList(DoubleColumn.create("adj_close", adjClose),
                                             DoubleColumn.create("close", close),
                                             DoubleColumn.create("open", open),
                                             DoubleColumn.create("high", high),
                                             DoubleColumn.create("low", low),
                                             StringColumn.create("symbol", symbol),
                                             LongColumn.create("volume", volume.stream().mapToLong(Long::longValue)),
                                             DateColumn.create("date", date.stream()));

        Table stockHistory = Table.create("google", cols);

        //System.out.println(googHistory.print());

        DoubleColumn normalizedPrices = stockHistory.doubleColumn("adj_close")
                                                   .divide(stockHistory.doubleColumn("adj_close").get(0))
                                                   .setName("normalized_prices");

        stockHistory.addColumns(normalizedPrices);
        System.out.println(stockHistory.print());

        return stockHistory;
    }




/*
    static Table normalizePrices(Table prices) throws IOException
    {
        // assumes table contains column = "date"

        DateColumn dateColumn = prices.dateColumn("date").copy();
        prices = prices.removeColumns("date");
        List<String> columnNames = prices.columnNames();

        DenseMatrix matrix = prices.smile().toDataFrame().toMatrix();

        double[] firstRow = matrix.toArray()[0];

        double[][] firstRowCopies = new double[matrix.nrows()][];
        for (int i = 0; i < firstRowCopies.length; ++i) {
            firstRowCopies[i] =  Arrays.copyOfRange(firstRow, 0, firstRow.length);
        }

        DenseMatrix normalizedPrices = matrix.div(Matrix.of(firstRowCopies));

        Table normalizedPricesTable = tableFromDenseMatrix(normalizedPrices, columnNames);

        normalizedPricesTable.addColumns(dateColumn);

        String[] reorderedCols = ArrayUtils.addAll(new String[]{"date"}, columnNames.toArray(new String[0]));

        return normalizedPricesTable.reorderColumns(reorderedCols); // put date column back in front
    }*/
}
