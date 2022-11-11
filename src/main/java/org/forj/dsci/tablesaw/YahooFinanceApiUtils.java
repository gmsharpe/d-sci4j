package org.forj.dsci.tablesaw;

import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class YahooFinanceApiUtils
{
    static Table getData(List<String> symbols) throws IOException
    {
        Calendar from = Calendar.getInstance();
        Calendar to = Calendar.getInstance();
        from.add(Calendar.YEAR, -1); // from 1 years ago

        Table table = null;

        List<Table> individualHistories = new ArrayList<>();

        for (String symbol : symbols) {
            Stock stock = YahooFinance.get(symbol.toUpperCase(), from, to, Interval.DAILY);
            List<HistoricalQuote> history = stock.getHistory();
            List<BigDecimal> adjClose = new ArrayList<>();
            List<LocalDate> date = new ArrayList<>();

            history.forEach(q -> {
                adjClose.add(q.getAdjClose());
                date.add(LocalDateTime.ofInstant(q.getDate().toInstant(), q.getDate().getTimeZone().toZoneId()).toLocalDate());
            });

            List<Column<?>> cols = Arrays.asList(DateColumn.create("date", date.stream()),
                                                 DoubleColumn.create(symbol, adjClose));

            Table stockHistory = Table.create(symbol, cols);
            individualHistories.add(stockHistory);
        }

        table = individualHistories.get(0);

        for (int x = 1; x < individualHistories.size(); x ++) {
            table = table.joinOn("date").fullOuter(individualHistories.get(x));
        }
        return table;
    }
}
