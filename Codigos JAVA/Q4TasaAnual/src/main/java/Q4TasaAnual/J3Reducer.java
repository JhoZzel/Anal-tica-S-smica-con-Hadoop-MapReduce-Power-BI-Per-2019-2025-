package Q4TasaAnual;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Reducer: genera ranking global ordenado por excedencias (descendente) */
public class J3Reducer extends Reducer<LongWritable, Text, Text, Text> {
    private boolean header = false;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        if (!header) {
            outKey.set("DEPARTAMENTO");
            outVal.set("Excedencias\tAnios\tEventos\tRate_anual\tT_ret_anios");
            ctx.write(outKey, outVal);
            header = true;
        }

        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 6) continue;
            String dep    = p[0];
            double rate   = Double.parseDouble(p[1]);
            long years    = Long.parseLong(p[2]);
            long exTot    = Long.parseLong(p[3]);
            long nTot     = Long.parseLong(p[4]);
            double T      = Double.parseDouble(p[5]);

            String Tstr = Double.isInfinite(T) ? "INF" : String.format(Locale.US, "%.3f", T);
            outKey.set(dep);
            outVal.set(String.format(Locale.US,
                    "%d\t%d\t%d\t%.6f\t%s", exTot, years, nTot, rate, Tstr));
            ctx.write(outKey, outVal);
        }
    }
}
