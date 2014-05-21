package com.ctriposs.sdb.benchmark;

import java.io.*;
import java.util.*;

/**
 * Created by yqdong on 14-5-19.
 */
public class ResultAggregator {

    private static final String[] operations = new String[]{"fillseq", "fillrandom", "fill100K", "readrandom"};

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("No result file provided.");
            return;
        }

        if (args.length % 2 != 0) {
            System.err.printf("No result file associated with %s\n", args[args.length - 1]);
            return;
        }

        List<TestResult> results = new ArrayList<TestResult>();
        for (int i = 0; i < args.length; i += 2) {
            TestResult result = readFromFile(args[i], args[i + 1]);
            if (result != null) {
                results.add(result);
            }
        }

        if (results.isEmpty()) {
            System.err.println("No result file loaded.");
            return;
        }

        writeHeader(System.out);
        for (TestResult result : results) {
            if (result != null) {
                writeTestResult(System.out, result);
            }
        }
    }

    private static TestResult readFromFile(String dbName, String filePath) {
        TestResult result = new TestResult();
        result.dbName = dbName;
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.printf("Unable to find %s\n", filePath);
            return null;
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));

            boolean inResultSection = false;
            while (reader.ready()) {
                String line = reader.readLine();
                if (inResultSection) {
                    // Deal with RocksDB
                    if (!line.contains("micros/op")) {
                        continue;
                    }

                    int colonIndex = line.indexOf(" :");
                    int microsOpIndex = line.indexOf(" micros/op");
                    String opName = line.substring(0, colonIndex).trim();
                    double opResult = Double.parseDouble(line.substring(colonIndex + 2, microsOpIndex).trim());

                    if (opName.contains("snap")) {
                        continue;
                    }

                    result.items.put(opName, opResult);
                } else if (line.startsWith("---")) {
                    inResultSection = true;
                }
            }
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            System.err.printf("Error occurred when reading result from %s: %s\n", filePath, e.getMessage());
            return null;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }
        }
        return result;
    }

    private static void writeHeader(PrintStream stream) {
        stream.print("                    ");
        for (String op : operations) {
            stream.printf(" %13s", op);
        }
        stream.println();
        stream.print("--------------------");
        for (int i = 0; i < operations.length; ++i) {
            stream.print("--------------");
        }
        stream.println("-");
    }

    private static void writeTestResult(PrintStream stream, TestResult result) {
        stream.printf("%-20s", result.dbName);
        for (String operation : operations) {
            Double cost = result.items.get(operation);
            stream.printf(" %13s", cost != null ? cost.toString() : "N/A");
        }
        stream.println();
    }

    private static class TestResult {

        public String dbName;
        public final Map<String, Double> items = new LinkedHashMap<String, Double>();
    }
}
