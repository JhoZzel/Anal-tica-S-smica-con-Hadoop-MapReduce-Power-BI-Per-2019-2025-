package Q5TendenciaMensual;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class J1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx)
            throws IOException, InterruptedException {

        String s = line.toString();
        if (s.isEmpty() || s.contains("FECHA_EVENTO")) return;

        String[] f = s.split(",");
        // Indices según tu dataset:
        // 2: DEPARTAMENTO, 5: FECHA_EVENTO (yyyymmdd), 11: NS, 12: EO
        if (f.length < 13) return;

        try {
            String dep = f[2].trim();
            String fecha = f[5].trim();       // "YYYYMMDD"
            if (fecha.length() < 6) return;
            String yyyymm = fecha.substring(0, 6);  // "YYYYMM"

            double ns = Double.parseDouble(f[11].trim());
            double eo = Double.parseDouble(f[12].trim());
            double hmax = Math.max(ns, eo);   // horizontal máxima

            outKey.set(dep + "|" + yyyymm);
            outVal.set(hmax + "\t1");
            ctx.write(outKey, outVal);

        } catch (Exception ex) {
            // ignora fila malformada
        }
    }
}
