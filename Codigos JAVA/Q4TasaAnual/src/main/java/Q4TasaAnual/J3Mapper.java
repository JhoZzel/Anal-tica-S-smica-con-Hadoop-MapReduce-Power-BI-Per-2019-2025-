package Q4TasaAnual;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Mapper: usa clave negativa para ordenar por excedencias descendente */
public class J3Mapper extends Mapper<Text, Text, LongWritable, Text> {
    private final LongWritable outKey = new LongWritable();
    private final Text outVal = new Text();

    @Override
    protected void map(Text dep, Text value, Context ctx) throws IOException, InterruptedException {
        // value = "avgRate|years|exTot|nTot"
        String[] p = value.toString().split("\\|");
        if (p.length < 4) return;
        try {
            double rate = Double.parseDouble(p[0]);
            long years  = Long.parseLong(p[1]);
            long exTot  = Long.parseLong(p[2]);
            long nTot   = Long.parseLong(p[3]);

            double T = (rate > 0.0) ? (1.0 / rate) : Double.POSITIVE_INFINITY;

            // Negativo para ordenar descendente (más excedencias primero)
            outKey.set(-exTot);

            // Incluimos todo lo necesario en el valor
            outVal.set(dep.toString() + "|" + rate + "|" + years + "|" + exTot + "|" + nTot + "|" + T);
            ctx.write(outKey, outVal);
        } catch (NumberFormatException ignore) {}
    }
}
