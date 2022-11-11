package org.forj.dsci.tablesaw;

import net.tlabs.tablesaw.parquet.*;
import org.apache.parquet.tools.command.ShowMetaCommand;
import tech.tablesaw.api.Table;
import static org.apache.parquet.tools.Main.main;

import java.io.IOException;

public class SimpleParquet
{
    static String HOME = "F:\\git-gms\\d-sci4j\\data\\tidyverse\\";

    public static void main(String[] args) throws IOException
    {

        Table table = Table.read().csv(HOME + "melt_example_2.csv");
        //System.out.println(table.print());

        TablesawParquet.register();

        //new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder(HOME + "melt_example.parquet").build());

        //Table table2 = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(HOME + "melt_example.parquet").build());
        //System.out.println(table2.print());
        org.apache.parquet.tools.Main.main(new String[]{"meta", HOME + "melt_example.parquet"});

        table.rowCount();



        new TablesawParquetWriter().write(table,
                                          TablesawParquetWriteOptions.builder(HOME + "melt_example.parquet").withOverwrite(true)
                                                                     .withCompressionCode(TablesawParquetWriteOptions.CompressionCodec.GZIP)
                                                                     .build());



/*        Table table2 = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(HOME + "melt_example.parquet")
                                                                                  .dateFormat()
                                                                                  .dateTimeFormat()
                                                                                  .missingValueIndicator()
                                                                                  .withConvertInt96ToTimestamp(true)
                                                                                  .withOnlyTheseColumns()
                                                                                  .build());*/
    }

}
