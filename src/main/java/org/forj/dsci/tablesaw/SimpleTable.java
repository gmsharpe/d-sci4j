package org.forj.dsci.tablesaw;

import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;


public class SimpleTable extends Table
{
    protected SimpleTable(String name, Column<?>... columns)
    {
        super(name, columns);
    }

}
