package vn.edu.ueh.bit.topten;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, Text, LongWritable> {
    private static final int TOP_K = 10;

    private TreeMap<Long, List<String>> topRecords;
    private int currentSize;

    @Override
    public void setup(Context context) {
        topRecords = new TreeMap<>();
        currentSize = 0;
    }

    @Override
    public void map(Object key, Text value, Context context) {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] tokens = line.split("\\s+");
        if (tokens.length < 2) {
            return;
        }

        try {
            String name = tokens[0];
            long amount = Long.parseLong(tokens[1]);
            addRecord(amount, name);
        } catch (NumberFormatException ignored) {
            // Skip malformed rows.
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, List<String>> entry : topRecords.descendingMap().entrySet()) {
            LongWritable amount = new LongWritable(entry.getKey());
            for (String name : entry.getValue()) {
                context.write(new Text(name), amount);
            }
        }
    }

    private void addRecord(long amount, String name) {
        topRecords.computeIfAbsent(amount, k -> new ArrayList<>()).add(name);
        currentSize++;
        trimToTopK();
    }

    private void trimToTopK() {
        while (currentSize > TOP_K && !topRecords.isEmpty()) {
            Map.Entry<Long, List<String>> smallest = topRecords.firstEntry();
            List<String> names = smallest.getValue();
            names.remove(0);
            currentSize--;
            if (names.isEmpty()) {
                topRecords.pollFirstEntry();
            }
        }
    }
}
