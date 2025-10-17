package Q5TendenciaMensual;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class J3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private final DoubleWritable outKey = new DoubleWritable();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx)
            throws IOException, InterruptedException {
        // Entrada: DEP \t slopeYear|intercept|R2|n|ybar
        String s = line.toString();
        String[] kv = s.split("\t");
        if (kv.length < 2) return;

        String dep = kv[0].trim();
        String[] p = kv[1].split("\\|");
        if (p.length < 5) return;

        double slopeYear = Double.parseDouble(p[0]);
        String intercept = p[1];
        String r2 = p[2];
        String n = p[3];
        String ybar = p[4];

        outKey.set(slopeYear);
        outVal.set(dep + "|" + intercept + "|" + r2 + "|" + n + "|" + ybar);
        ctx.write(outKey, outVal);
    }
}
