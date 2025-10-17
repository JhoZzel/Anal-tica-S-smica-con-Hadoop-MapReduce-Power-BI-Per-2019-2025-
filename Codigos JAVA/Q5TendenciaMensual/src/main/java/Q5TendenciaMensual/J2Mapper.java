package Q5TendenciaMensual;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class J2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx)
            throws IOException, InterruptedException {
        String s = line.toString();
        if (s.isEmpty()) return;

        // Formato entrada: DEP \t YYYYMM|mean
        String[] kv = s.split("\t");
        if (kv.length < 2) return;

        outKey.set(kv[0].trim());
        outVal.set(kv[1].trim()); // "YYYYMM|mean"
        ctx.write(outKey, outVal);
    }
}
