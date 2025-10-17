/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Q6RegresionLineal;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();
    private boolean byDept;

    @Override
    protected void setup(Context ctx) {
        String mode = ctx.getConfiguration().get("reg.groupby", "GLOBAL");
        byDept = "DEPT".equalsIgnoreCase(mode);
    }

    @Override
    protected void map(LongWritable key, Text line, Context ctx)
            throws IOException, InterruptedException {

        String s = line.toString();
        // Evitar encabezado y filas vacías/cortas
        if (s.isEmpty() || s.contains("ACEL_MAX_VERTICAL") || s.contains("FECHA_CORTE")) return;

        String[] f = s.split(",");
        if (f.length < 13) return;

        try {
            String dep = f[2].trim();
            double y  = Double.parseDouble(f[10].trim()); // ACEL_MAX_VERTICAL
            double x1 = Double.parseDouble(f[11].trim()); // ACEL_MAX_NORTE-SUR
            double x2 = Double.parseDouble(f[12].trim()); // ACEL_MAX_ESTE-OESTE

            // Clave: ALL (global) o departamento
            outKey.set(byDept ? dep : "ALL");

            // Suficientes estadísticas por registro:
            // n|sx1|sx2|sy|sx1x1|sx2x2|sx1x2|sx1y|sx2y|sy2
            String packed = String.format(Locale.US,
                "%.0f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f",
                1.0, x1, x2, y, x1*x1, x2*x2, x1*x2, x1*y, x2*y, y*y);

            outVal.set(packed);
            ctx.write(outKey, outVal);

        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            // Si hay valores no numéricos o línea corta: ignorar
        }
    }
}
