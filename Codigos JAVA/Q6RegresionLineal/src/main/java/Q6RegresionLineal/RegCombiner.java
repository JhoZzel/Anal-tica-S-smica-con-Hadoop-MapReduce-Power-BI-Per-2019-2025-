/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Q6RegresionLineal;

/**
 *
 * @author jhozzel
 */

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegCombiner extends Reducer<Text, Text, Text, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {
        double[] s = new double[10]; // n, sx1, sx2, sy, sx1x1, sx2x2, sx1x2, sx1y, sx2y, sy2

        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 10) continue;
            for (int i = 0; i < 10; i++) {
                s[i] += Double.parseDouble(p[i]);
            }
        }

        String packed = String.format(Locale.US,
            "%.0f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f|%.10f",
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9]);

        outVal.set(packed);
        ctx.write(key, outVal);
    }
}
