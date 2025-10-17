package Q4TasaAnual;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class J1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final double TAU = 0.2; // <-- umbral fijo aquí (puedes cambiarlo)
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx) throws IOException, InterruptedException {
        String s = line.toString();
        if (s.isEmpty()) return;

        // Evita cabecera de forma segura
        if (s.startsWith("FECHA_CORTE") || s.contains("ACEL_MAX_VERTICAL")) return;

        String[] f = s.split(",");
        if (f.length <= 12) return;

        try {
            String dep = f[2].trim();
            String fecha = f[5].trim();          // YYYYMMDD
            if (fecha.length() < 8) return;
            String year = fecha.substring(0, 4);  // YYYY

            double ns = Double.parseDouble(f[11].trim());
            double ew = Double.parseDouble(f[12].trim());

            // H horizontal: usa hypot (estable) o max(ns, ew) si prefieres
            double H = Math.max(ns, ew);

            int ex = (H >= TAU) ? 1 : 0;

            // Clave por depto|año; valor "ex|1"
            outKey.set(dep + "|" + year);
            outVal.set(ex + "|" + 1);
            ctx.write(outKey, outVal);

        } catch (NumberFormatException ignore) {
            // línea malformada; la saltamos
        }
    }
}
