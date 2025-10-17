package Q4TasaAnual;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class J1Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        // key = "DEPARTAMENTO|YYYY"
        String[] ky = key.toString().split("\\|");
        if (ky.length < 2) return;
        String dep  = ky[0];
        String year = ky[1];

        long exSum = 0, nSum = 0;
        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 2) continue;
            exSum += Long.parseLong(p[0]);
            nSum  += Long.parseLong(p[1]);
        }

        // Salida para MR2: key=DEPARTAMENTO, value="YYYY|exSum|nSum"
        outKey.set(dep);
        outVal.set(year + "|" + exSum + "|" + nSum);
        ctx.write(outKey, outVal);
    }
}
