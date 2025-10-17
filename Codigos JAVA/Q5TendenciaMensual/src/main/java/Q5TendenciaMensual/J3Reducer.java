package Q5TendenciaMensual;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class J3Reducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private boolean header = false;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(DoubleWritable slopeKey, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        if (!header) {
            outKey.set("DEPARTAMENTO");
            outVal.set("Slope_anual\tR2\tMeses\tMean_mensual");
            ctx.write(outKey, outVal);
            header = true;
        }

        double slope = slopeKey.get();
        String slopeStr = String.format(Locale.US, "%.6f", slope);

        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 5) continue;
            String dep = p[0];
            String intercept = p[1]; // no lo imprimimos para no cargar la tabla
            String r2 = p[2];
            String n = p[3];
            String ybar = p[4];

            outKey.set(dep);
            outVal.set(String.format(Locale.US, "%s\t%s\t%s\t%s", slopeStr, r2, n, ybar));
            ctx.write(outKey, outVal);
        }
    }

    /** Comparador para ordenar DoubleWritable en orden DESCENDENTE */
    public static class DescDoubleComparator extends WritableComparator {
        protected DescDoubleComparator() {
            super(DoubleWritable.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable d1 = (DoubleWritable) a;
            DoubleWritable d2 = (DoubleWritable) b;
            return -d1.compareTo(d2); // invertir para descendente
        }
    }
}
