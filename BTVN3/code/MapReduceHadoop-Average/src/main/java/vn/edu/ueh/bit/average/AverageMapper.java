package vn.edu.ueh.bit.average;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final Text outKey = new Text();
    private final LongWritable outValue = new LongWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] tokens = line.split("\\s+");
        if (tokens.length < 2) {
            return;
        }

        try {
            outKey.set(tokens[0]);
            outValue.set(Long.parseLong(tokens[1]));
            context.write(outKey, outValue);
        } catch (NumberFormatException ignored) {
            // Skip malformed rows.
        }
    }
}
