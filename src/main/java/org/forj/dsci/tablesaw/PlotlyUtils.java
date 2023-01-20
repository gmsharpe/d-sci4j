package org.forj.dsci.tablesaw;

import tech.tablesaw.plotly.components.Figure;

public class PlotlyUtils
{
    public static final String pageTop =
            "<html>"
                    + System.lineSeparator()
                    + "<head>"
                    + System.lineSeparator()
                    + "    <title>Multi-plot test</title>"
                    + System.lineSeparator()
                    + "    <script src=\"https://cdn.plot.ly/plotly-latest.min.js\"></script>"
                    + System.lineSeparator()
                    + "</head>"
                    + System.lineSeparator()
                    + "<body>"
                    + System.lineSeparator()
                    + "<div id='plot1'>"
                    + System.lineSeparator()
                    + "<div id='plot2'>"
                    + System.lineSeparator();

    public static final String pageBottom = "</body>" + System.lineSeparator() + "</html>";

    public static String makePage(Figure figure1, Figure figure2, String divName1, String divName2) {
        return new StringBuilder()
                .append(pageTop)
                .append(System.lineSeparator())
                .append(figure1.asJavascript(divName1))
                .append(System.lineSeparator())
                .append(figure2.asJavascript(divName2))
                .append(System.lineSeparator())
                .append(pageBottom)
                .toString();
    }
}
