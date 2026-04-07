package vn.edu.ueh.bit.temp;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

public class GenerateData {
    public void gen(String path, long records) throws Exception {
        Random rand = new Random();
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(path))) {
            for (int i = 0; i < records; i++) {
                int randTransId = rand.nextInt(100);
                int randValue = rand.nextInt(10000);
                pw.println("Transaction#" + randTransId + " " + randValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String outputPath = args.length > 0 ? args[0] : "trans.txt";
        long records = args.length > 1 ? Long.parseLong(args[1]) : 1_000_000L;

        GenerateData gen = new GenerateData();
        gen.gen(outputPath, records);
        System.out.println("Done: " + outputPath + " (" + records + " records)");
    }
}
