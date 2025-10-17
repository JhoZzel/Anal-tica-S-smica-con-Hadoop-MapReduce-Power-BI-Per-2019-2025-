package Q4TasaAnual;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Lee (dep \t YYYY|ex|n), calcula r_year = ex/n y emite (dep -> r|1|ex|n) */
public class J2Mapper extends Mapper<Text, Text, Text, Text> {
    private final Text outVal = new Text();

    @Override
    protected void map(Text dep, Text value, Context ctx) throws IOException, InterruptedException {
        // value = "YYYY|ex|n"
        String[] p = value.toString().split("\\|");
        if (p.length < 3) return;
        try {
            long ex = Long.parseLong(p[1]);
            long n  = Long.parseLong(p[2]);
            double rYear = (n > 0) ? ((double) ex) / n : 0.0;

            // r_year | 1 | ex | n
            outVal.set(rYear + "|" + 1 + "|" + ex + "|" + n);
            ctx.write(dep, outVal);
        } catch (NumberFormatException ignore) {}
    }
}
