package vn.edu.ueh.bit.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {
    private final DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        long count = 0L;

        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }

        if (count == 0L) {
            return;
        }

        result.set((double) sum / count);
        context.write(key, result);
    }
}
