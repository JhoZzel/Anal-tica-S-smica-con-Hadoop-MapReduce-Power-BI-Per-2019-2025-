package Q4TasaAnual;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Promedia r_year por dep: avgRate = sum(r)/count; acumula exTot y nTot */
public class J2Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text dep, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        double sumR = 0.0;
        long countYears = 0;
        long exTot = 0;
        long nTot  = 0;

        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 4) continue;
            sumR       += Double.parseDouble(p[0]); // r_year
            countYears += Long.parseLong(p[1]);     // 1
            exTot      += Long.parseLong(p[2]);
            nTot       += Long.parseLong(p[3]);
        }

        double avgRate = (countYears > 0) ? (sumR / countYears) : 0.0;
        // value = avgRate|years|exTot|nTot
        outVal.set(avgRate + "|" + countYears + "|" + exTot + "|" + nTot);
        ctx.write(dep, outVal);
    }
}
