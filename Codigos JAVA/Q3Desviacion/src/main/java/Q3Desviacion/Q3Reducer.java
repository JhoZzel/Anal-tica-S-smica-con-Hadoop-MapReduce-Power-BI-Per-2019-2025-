package Q3Desviacion;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Q3Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text();
    private boolean headerWritten = false;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        if (!headerWritten) {
            ctx.write(new Text("DEPARTAMENTO"),
                      new Text("Desv_ACEL_VERT\tDesv_ACEL_NS\tDesv_ACEL_EO"));
            headerWritten = true;
        }
        
        long c = 0;
        double sumV=0, sumNS=0, sumEW=0;
        double sum2V=0, sum2NS=0, sum2EW=0;
        
        for (Text t : values) {
            String[] p = t.toString().split("\t");
            if (p.length==4) {
                
                double v  = Double.parseDouble(p[0]);
                double ns = Double.parseDouble(p[1]);
                double ew = Double.parseDouble(p[2]);
                sumV += v; sum2V += v * v;
                sumNS += ns; sum2NS += ns * ns;
                sumEW += ew; sum2EW += ew * ew;
                
                c   += Long.parseLong(p[3]);
            }
        }
        double stdV = Math.sqrt(sum2V / c - (sumV / c) * (sumV / c));
        double stdNS = Math.sqrt(sum2NS / c - (sumNS / c) * (sumNS / c));
        double stdEW = Math.sqrt(sum2EW / c - (sumEW / c) * (sumEW / c));
           
        outVal.set(String.format("%.6f\t%.6f\t%.6f", stdV, stdNS, stdEW));
        ctx.write(key, outVal);
    }
}
