### JShell

https://docs.oracle.com/en/java/javase/11/jshell/introduction-jshell.html

### JShell Maven Plugin

https://github.com/johnpoth/jshell-maven-plugin

#### Start d-sci4j

    .\pmaven.bat
    
    set JAVA_HOME=E:\jdk\jdk-11.0.6 
    REM mvn install
    mvn -q jshell:run -Djshell.scripts="config/startup/default-startup"    

## Tablesaw 

* https://github.com/jtablesaw/tablesaw
* https://jtablesaw.github.io/tablesaw/gettingstarted.html


#### Columns

    jshell> double[] numbers = {1, 2, 3, 4};
    numbers ==> double[4] { 1.0, 2.0, 3.0, 4.0 }

    jshell> DoubleColumn nc = DoubleColumn.create("nc", numbers);
    nc ==> Double column: nc

    jshell> System.out.println(nc.print());
    Column: nc
    1
    2
    3
    4

#### Importing Data

    jshell> Table bushTable = Table.read().csv("data/bush.csv");
    
    jshell> System.out.println(bushTable.structure());

    jshell> System.out.println(bushTable.last(3))
    
                  bush.csv
        date     |  approval  |   who   |
    -------------------------------------
     2001-03-27  |        52  |  zogby  |
     2001-02-27  |        53  |  zogby  |
     2001-02-09  |        57  |  zogby  |


    jshell> System.out.println(bushTable.printAll())

                bush.csv
        date     |  approval  |    who     |
    ----------------------------------------
     2004-02-04  |        53  |       fox  |
     2004-01-21  |        53  |       fox  |
     2004-01-07  |        58  |       fox  |
     2003-12-03  |        52  |       fox  |
     2003-11-18  |        52  |       fox  |
     2003-10-28  |        53  |       fox  |
    ...... more records

