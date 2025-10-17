package Q5TendenciaMensual;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class J1Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        double sum = 0.0;
        long cnt = 0L;

        for (Text t : values) {
            String[] p = t.toString().split("\t");
            if (p.length < 2) continue;
            sum += Double.parseDouble(p[0]);
            cnt += Long.parseLong(p[1]);
        }
        if (cnt == 0) return;

        // key = "DEP|YYYYMM"
        String[] kp = key.toString().split("\\|");
        if (kp.length != 2) return;
        String dep = kp[0];
        String yyyymm = kp[1];

        double mean = sum / cnt;

        outKey.set(dep);
        outVal.set(yyyymm + "|" + String.format(java.util.Locale.US, "%.6f", mean));
        ctx.write(outKey, outVal);
    }
}
