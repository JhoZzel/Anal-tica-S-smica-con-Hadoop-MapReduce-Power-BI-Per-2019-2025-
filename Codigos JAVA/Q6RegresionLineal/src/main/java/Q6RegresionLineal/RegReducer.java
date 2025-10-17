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

public class RegReducer extends Reducer<Text, Text, Text, Text> {

    private boolean headerWritten = false;
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        double n=0, sx1=0, sx2=0, sy=0, sx1x1=0, sx2x2=0, sx1x2=0, sx1y=0, sx2y=0, sy2=0;

        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 10) continue;
            n     += Double.parseDouble(p[0]);
            sx1   += Double.parseDouble(p[1]);
            sx2   += Double.parseDouble(p[2]);
            sy    += Double.parseDouble(p[3]);
            sx1x1 += Double.parseDouble(p[4]);
            sx2x2 += Double.parseDouble(p[5]);
            sx1x2 += Double.parseDouble(p[6]);
            sx1y  += Double.parseDouble(p[7]);
            sx2y  += Double.parseDouble(p[8]);
            sy2   += Double.parseDouble(p[9]);
        }

        long nInt = (long)Math.round(n);

        // X'X
        double a=n, b=sx1, c=sx2, d=sx1, e=sx1x1, f=sx1x2, g=sx2, h=sx1x2, i=sx2x2;
        // det(X'X)
        double det = a*(e*i - f*h) - b*(d*i - f*g) + c*(d*h - e*g);

        String beta0s="NA", beta1s="NA", beta2s="NA", r2s="NA";

        if (Math.abs(det) >= 1e-12 && nInt >= 3) {
            // Cofactores (adjunta transpuesta)
            double A11 =  (e*i - f*h);
            double A12 = -(d*i - f*g);
            double A13 =  (d*h - e*g);
            double A21 = -(b*i - c*h);
            double A22 =  (a*i - c*g);
            double A23 = -(a*h - b*g);
            double A31 =  (b*f - c*e);
            double A32 = -(a*f - c*d);
            double A33 =  (a*e - b*d);

            // (X'X)^{-1} = (1/det) * adj
            double inv00 = A11/det, inv01 = A21/det, inv02 = A31/det;
            double inv10 = A12/det, inv11 = A22/det, inv12 = A32/det;
            double inv20 = A13/det, inv21 = A23/det, inv22 = A33/det;

            // beta = inv * X'y
            double beta0 = inv00*sy + inv01*sx1y + inv02*sx2y;
            double beta1 = inv10*sy + inv11*sx1y + inv12*sx2y;
            double beta2 = inv20*sy + inv21*sx1y + inv22*sx2y;

            // R^2
            double ybar = sy / n;
            double TSS  = sy2 - n*ybar*ybar;
            double SSR  = beta0*sy + beta1*sx1y + beta2*sx2y - n*ybar*ybar;
            double R2   = (TSS > 1e-12) ? Math.max(0.0, Math.min(1.0, SSR/TSS)) : 1.0;

            beta0s = String.format(Locale.US, "%.6f", beta0);
            beta1s = String.format(Locale.US, "%.6f", beta1);
            beta2s = String.format(Locale.US, "%.6f", beta2);
            r2s    = String.format(Locale.US, "%.6f", R2);
        }

        if (!headerWritten) {
            ctx.write(new Text("KEY"),
                      new Text("beta0\tbeta1(NS)\tbeta2(EO)\tR2\tn"));
            headerWritten = true;
        }

        String out = beta0s + "\t" + beta1s + "\t" + beta2s + "\t" + r2s + "\t" + nInt;
        outVal.set(out);
        ctx.write(key, outVal);
    }
}
