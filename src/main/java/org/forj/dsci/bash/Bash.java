package org.forj.dsci.bash;

import java.io.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Bash
{

    public static void main(String[] args) throws IOException, InterruptedException
    {
        executeBashCommand("ls -l");

        Consumer<String> bash = command -> {
            System.out.println("Executing BASH command:\n   " + command);
            Runtime r = Runtime.getRuntime();
            String[] commands = {"bash", "-c", command};
            try {
                Process p = r.exec(commands);

                p.waitFor();
                BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line = "";

                while ((line = b.readLine()) != null) {
                    System.out.println(line);
                }

                b.close();
            } catch (Exception e) {
                System.err.println("Failed to execute bash with command: " + command);
                e.printStackTrace();
            }
        };

    }

    public static void executeCommands() throws IOException, InterruptedException
    {

        File tempScript = createTempScript();

        try {
            ProcessBuilder pb = new ProcessBuilder("bash", tempScript.toString());
            pb.inheritIO();
            Process process = pb.start();
            process.waitFor();
        } finally {
            tempScript.delete();
        }
    }

    public static File createTempScript() throws IOException {
        File tempScript = File.createTempFile("script", null);

        Writer streamWriter = new OutputStreamWriter(new FileOutputStream(
                tempScript));
        PrintWriter printWriter = new PrintWriter(streamWriter);

        printWriter.println("#!/bin/bash");
        printWriter.println("cd bin");
        printWriter.println("ls");

        printWriter.close();

        return tempScript;
    }



    /**
     * Execute a bash command. We can handle complex bash commands including
     * multiple executions (; | && ||), quotes, expansions ($), escapes (\), e.g.:
     *     "cd /abc/def; mv ghi 'older ghi '$(whoami)"
     * @param command
     * @return true if bash got started, but your command may have failed.
     */
    public static boolean executeBashCommand(String command) {
        boolean success = false;
        System.out.println("Executing BASH command:\n   " + command);
        Runtime r = Runtime.getRuntime();
        // Use bash -c so we can handle things like multi commands separated by ; and
        // things like quotes, $, |, and \. My tests show that command comes as
        // one argument to bash, so we do not need to quote it to make it one thing.
        // Also, exec may object if it does not have an executable file as the first thing,
        // so having bash here makes it happy provided bash is installed and in path.
        String[] commands = {"bash", "-c", command};
        try {
            Process p = r.exec(commands);
            p.waitFor();
            BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";

            while ((line = b.readLine()) != null) {
                System.out.println(line);
            }

            b.close();
            success = true;
        } catch (Exception e) {
            System.err.println("Failed to execute bash with command: " + command);
            e.printStackTrace();
        }
        return success;
    }

}
