package org.forj.dsci.tablesaw;

import org.apache.commons.io.FileUtils;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.ColumnTypeDetector;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SimpleRow
{
    private void t() throws IOException
    {
        int year = 2021;
        int[] quarters = new int[]{1, 2, 3, 4};

        Map<Integer, Map<Integer, Map<String, Table>>> tables = new HashMap<Integer, Map<Integer, Map<String, Table>>>();
        tables.put(year, new HashMap<Integer, Map<String, Table>>());
        IntStream.of(quarters).forEach(q -> tables.get(year).put(q, new HashMap<String, Table>()));

        String[] tableNames = new String[]{"sub","pre","num"};

        Table table = Table.read()
                           .csv(CsvReadOptions.builderFromFile("fileName")
                                              .maxCharsPerColumn(20000)
                                              .separator('\t').build());


        for(String tableName : tableNames) {
            for(int quarter : quarters){
                String fileName = year + "q" + quarter + "/" + tableName + ".txt";
                Table tempTable = Table.read()
                                   .csv(CsvReadOptions.builderFromFile(fileName)
                                                      .maxCharsPerColumn(20000)
                                                      .separator('\t').build());
                table.append(table);
            }
        }


    }

    public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }
    
    private void x() throws IOException
    {
        String url = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q1.zip";
        String zippedFile = "/content/2022q1.zip";
        FileUtils.copyURLToFile(new URL(url), new File(zippedFile));

        File destDir = new File("/content/2022q1");        byte[] buffer = new byte[1024];
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zippedFile))) {

            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                File newFile = newFile(destDir, zipEntry);
                if (zipEntry.isDirectory()) {
                    if (!newFile.isDirectory() && !newFile.mkdirs()) {
                        throw new IOException("Failed to create directory " + newFile);
                    }
                } else {
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                    zipEntry = zis.getNextEntry();
                }
            }
        }
    }
}

