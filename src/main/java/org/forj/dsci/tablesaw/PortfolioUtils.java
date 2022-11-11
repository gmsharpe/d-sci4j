package org.forj.dsci.tablesaw;

import org.apache.commons.lang3.ArrayUtils;
import smile.data.DataFrame;
import smile.io.CSV;
import smile.math.matrix.DenseMatrix;
import smile.math.matrix.Matrix;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class PortfolioUtils
{
    static double getStdDailyReturns(Table dailyReturns) throws IOException
    {
        DenseMatrix matrix = Matrix.of(dailyReturns.copy().removeColumns("date").smile()
                                                   .toDataFrame()
                                                   .slice(1, dailyReturns.rowCount())
                                                   .toMatrix()
                                                   .colSds())
                                   .transpose();

        return matrix.get(0, 0);
    }

    static double getCumulativeReturn(Table portfolioValues) throws IOException
    {
        // (portfolio_value[-1] / portfolio_value[0]) - 1
        portfolioValues = portfolioValues.copy().removeColumns("date");
        List<String> columnNames = portfolioValues.columnNames();
        Row first = portfolioValues.row(0);
        Row last = portfolioValues.row(portfolioValues.rowCount() - 1);
        double[] results = new double[portfolioValues.columnCount()];
        for (int x = 0; x < portfolioValues.columnCount(); x++) {
            double cumReturn = (first.getDouble(x) / last.getDouble(x)) - 1;
            results[x] = cumReturn;
        }
        return Matrix.of(results).get(0, 0);
    }

    static double sharpe(Table prices, double[] allocations) throws IOException
    {
        /*
            port_val = (normalized_prices * allocs).sum(axis=1)
            daily_returns = port_val.pct_change(1)
            daily_returns[0] = 0
            adr = daily_returns[1:].mean()
            sddr = daily_returns[1:].std()
            sr = 252 ** .5 * ((adr - 0) / sddr)
            return sr
         */
        Table dailyReturns = getDailyReturns(getPortfolioValue(normalizePrices(prices), allocations));
        double avgDailyReturns = getAverageDailyReturns(dailyReturns.copy());
        double standardDeviationDailyReturns = getStdDailyReturns(dailyReturns.copy());
        double sharpe = ((avgDailyReturns - 0) / standardDeviationDailyReturns) * Math.pow(252, .5);

        return sharpe;
    }

    static Table normalizePrices(Table prices) throws IOException
    {
        return calculateStatistic(prices, (DenseMatrix matrix) -> {
            double[] firstRow = matrix.toArray()[0];

            double[][] firstRowCopies = new double[matrix.nrows()][];
            for (int i = 0; i < firstRowCopies.length; ++i) {
                firstRowCopies[i] = Arrays.copyOfRange(firstRow, 0, firstRow.length);
            }

            return matrix.div(Matrix.of(firstRowCopies));
        });
    }

    static Table tableFromDenseMatrix(DenseMatrix matrix, List<String> columns) throws IOException
    {
        Path tempPath = Files.createTempFile("", ".tmp.csv");

        new CSV().write(DataFrame.of(matrix.toArray(), columns.toArray(new String[0])), tempPath);

        Table normalizedPricesTable = Table.read().csv(tempPath.toFile());

        Files.deleteIfExists(tempPath);

        return normalizedPricesTable;
    }

    static Table getPortfolioValue(Table normalizedPrices, double[] allocations) throws IOException
    {

        DateColumn dateColumn = normalizedPrices.dateColumn("date");

        DenseMatrix matrix = normalizedPrices.copy().removeColumns("date").smile().toDataFrame().toMatrix();

        double[][] allocationsMatrix = new double[normalizedPrices.rowCount()][];
        for (int i = 0; i < allocationsMatrix.length; ++i) {
            allocationsMatrix[i] =  Arrays.copyOfRange(allocations, 0, allocations.length);
        }

        DenseMatrix portfolioValues = Matrix.of(matrix.mul(Matrix.of(allocationsMatrix)).rowSums());

        Table portfolioValuesTable = tableFromDenseMatrix(portfolioValues, Arrays.asList("port_value"));

        portfolioValuesTable.addColumns(dateColumn);

        return portfolioValuesTable.reorderColumns(new String[]{"date", "port_value"}); // put date column back in front
    }

    static Table calculateStatistic(Table table, Function<DenseMatrix, DenseMatrix> function) throws IOException
    {
        DateColumn dateColumn = table.dateColumn("date").copy();
        List<String> columnNames = table.columnNames();
        columnNames.remove("date");
        Table justPrices = table.selectColumns(columnNames.toArray(new String[0]));

        DenseMatrix matrix = function.apply(justPrices.smile().toDataFrame().toMatrix());

        Table tableResult = tableFromDenseMatrix(matrix, columnNames);

        tableResult.addColumns(dateColumn);

        String[] reorderedCols = ArrayUtils.addAll(new String[]{"date"}, columnNames.toArray(new String[0]));

        return tableResult.reorderColumns(reorderedCols); // put date column back in front
    }

    static Table getDailyReturns(Table portfolioValues) throws IOException
    {
        return calculateStatistic(portfolioValues, matrix -> {
            double[][] originalMatrixArray = matrix.toArray();
            // (closing - opening) / opening
            // today's close - day prior's close / day prior's close
            double[][] matrixArray = matrix.copy().toArray();

            for (int x = 0; x < (matrix.nrows() - 1); x++) {
                smile.math.MathEx.sub(matrixArray[x + 1], originalMatrixArray[x]);
                for (int y = 0; y < matrix.ncols(); y++) {
                    matrixArray[x + 1][y] = matrixArray[x + 1][y] / originalMatrixArray[x][y];
                }
            }

            return Matrix.of(matrixArray);
        });
    }

    static double getAverageDailyReturns(Table dailyReturns) throws IOException
    {
        DenseMatrix matrix = Matrix.of(dailyReturns.copy().removeColumns("date").smile()
                                                   .toDataFrame()
                                                   .slice(1, dailyReturns.rowCount())
                                                   .toMatrix()
                                                   .colMeans())
                                   .transpose();

        return matrix.get(0, 0);
    }

    public static Table getDateIndexedTableOfZeros(Table table) throws IOException
    {
        DateColumn dateColumn = table.dateColumn("date").copy();
        List<String> columnNames = table.columnNames();
        columnNames.remove("date");

        DenseMatrix zeros = Matrix.zeros(table.rowCount(), columnNames.size());

        Table tableResult = tableFromDenseMatrix(zeros, columnNames);

        tableResult.addColumns(dateColumn);

        String[] reorderedCols = ArrayUtils.addAll(new String[]{"date"}, columnNames.toArray(new String[0]));

        return tableResult.reorderColumns(reorderedCols); // put date column back in front
    }
}
