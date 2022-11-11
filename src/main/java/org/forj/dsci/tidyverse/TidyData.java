package org.forj.dsci.tidyverse;

import org.apache.commons.lang3.ArrayUtils;
import org.sparkproject.dmg.pmml.Aggregate;
import tech.tablesaw.aggregate.AggregateFunction;
import tech.tablesaw.aggregate.StringAggregateFunction;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TidyData
{
    static String HOME = "F:\\git-gms\\d-sci4j\\data\\tidyverse\\";
    public static void main(String[] args) throws IOException
    {
        meltExample2();
    }

    public static void customMelt() throws IOException
    {
        String[] expectedCols = { "religion", "income", "freq" };


        Table table = Table.read().csv(HOME + "needs_melting.csv");
        System.out.println(table.print());


        List<String> income = table.columnNames().stream().filter(c -> !c.equalsIgnoreCase("religion")).collect(
                Collectors.toList());

        int len = table.rowCount();

        String[] religionCol = new String[len * income.size()];
        Integer[] freqCol = new Integer[len * income.size()];
        String[] incomeCol = new String[len * income.size()];


        AtomicInteger x = new AtomicInteger(0);
        table.stream().parallel().forEach( row -> {
                                               income.stream().forEach(i -> {
                                                   String religion = row.getString("religion");
                                                   int rownum = x.getAndIncrement();
                                                   freqCol[rownum] = row.getInt(i);
                                                   incomeCol[rownum] = i;
                                                   religionCol[rownum] = religion;

                                               });
                                           });

        table.stream().parallel().forEach(row -> System.out.println(row));


        Table newtable = Table.create("molten", Arrays.asList(StringColumn.create("religion").addAll(Arrays.asList(religionCol)),
                                                              IntColumn.create("freq", freqCol),
                                                              StringColumn.create("income").addAll(Arrays.asList(incomeCol))));

        System.out.println(newtable.print());
        System.out.println(newtable.stream().count());
    }

    public static void meltExample() throws IOException
    {
        /*
         * <p>Tidy concepts: {@see https://www.jstatsoft.org/article/view/v059i10}
         *
         * <p>Cast function details: {@see https://www.jstatsoft.org/article/view/v021i12}
         */

        Table table = Table.read().csv(HOME + "needs_melting.csv");
        System.out.println(table.print());


        List<String> colVars = Arrays.asList("religion");
        List<String> colVals = Arrays.asList("<$10k", "$10-20k", "$30-40k", "$50-75k");

        Table molten = table.melt(colVars,
                                  table.numericColumns(colVals.toArray(String[]::new)),
                                  false);

        System.out.println(molten.print());
    }


    public static void meltExample2() throws IOException
    {
        Table table = Table.read().csv(HOME + "melt_example_2.csv");
        System.out.println(table.print());

        table.column("date.entered").setName("date");

        String[] colVars = new String[]{"year", "artist", "track", "time", "date"};
        String[] colVals = new String[]{"wk1", "wk2", "wk3"};

/*        List<Table> meltedTables = Arrays.stream(colVals).map( wk -> {
            Table subset = table.selectColumns(ArrayUtils.add(colVars, wk));
            return subset.melt(Arrays.asList(colVars), table.numericColumns(wk), false);
        }).collect(Collectors.toList());


        Table result = meltedTables.get(0)
                                   .append(meltedTables.get(1))
                                   .append(meltedTables.get(2));

 */

        Table result = table.melt(Arrays.asList(colVars), table.numericColumns(colVals), false);
        
        System.out.println(result.print());

        result.column("variable").setName("week");
        result.column("value").setName("rank");


        result.stream().forEach(r -> r.setString("week",
                                                 r.getString("week").replace("wk","")));


        System.out.println(result.print());



    }
}
