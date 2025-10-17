package Q1Promedio;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text();
    private boolean headerWritten = false;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        if (!headerWritten) {
            ctx.write(new Text("DEPARTAMENTO"),
                      new Text("Prom_ACEL_VERT\tProm_ACEL_NS\tProm_ACEL_EO"));
            headerWritten = true;
        }
        
        double sv=0, sns=0, sew=0; long c=0;
        for (Text t : values) {
            String[] p = t.toString().split("\t");
            if (p.length==4) {
                sv  += Double.parseDouble(p[0]);
                sns += Double.parseDouble(p[1]);
                sew += Double.parseDouble(p[2]);
                c   += Long.parseLong(p[3]);
            }
        }
        if (c==0) return;
        double avgV  = sv/c, avgNS = sns/c, avgEW = sew/c;
        outVal.set(String.format("%.6f\t%.6f\t%.6f", avgV, avgNS, avgEW));
        ctx.write(key, outVal);
    }
}
