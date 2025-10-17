package Q5TendenciaMensual;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class J2Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text dep, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        // Guardamos pares (mes, mean)
        List<int[]> months = new ArrayList<>(); // [YYYYMM]
        List<Double> means = new ArrayList<>();

        for (Text t : values) {
            String[] p = t.toString().split("\\|");
            if (p.length < 2) continue;
            try {
                int yyyymm = Integer.parseInt(p[0]);
                double mean = Double.parseDouble(p[1]);
                months.add(new int[]{yyyymm});
                means.add(mean);
            } catch (NumberFormatException ignore) {}
        }

        int n = means.size();
        if (n < 2) return; // no se puede ajustar con 1 punto

        // Ordenar por YYYYMM
        List<Integer> idx = new ArrayList<>();
        for (int i = 0; i < n; i++) idx.add(i);
        idx.sort((i, j) -> Integer.compare(months.get(i)[0], months.get(j)[0]));

        // x = 0..n-1 (índice mensual relativo), y = mean ordenado
        double sumX = 0, sumX2 = 0, sumY = 0, sumXY = 0;
        double[] ys = new double[n];

        for (int r = 0; r < n; r++) {
            int i = idx.get(r);
            double x = r;
            double y = means.get(i);
            ys[r] = y;
            sumX += x;
            sumX2 += x * x;
            sumY += y;
            sumXY += x * y;
        }

        double nD = n;
        double denom = (nD * sumX2 - sumX * sumX);
        if (denom == 0) return;

        double slope_per_month = (nD * sumXY - sumX * sumY) / denom;
        double xbar = sumX / nD;
        double ybar = sumY / nD;
        double intercept = ybar - slope_per_month * xbar;

        // R²
        double sst = 0, sse = 0;
        for (int r = 0; r < n; r++) {
            double x = r;
            double yhat = intercept + slope_per_month * x;
            sse += (ys[r] - yhat) * (ys[r] - yhat);
            sst += (ys[r] - ybar) * (ys[r] - ybar);
        }
        double r2 = (sst == 0) ? 0.0 : 1.0 - (sse / sst);

        double slope_per_year = slope_per_month * 12.0;

        outVal.set(String.format(
            java.util.Locale.US,
            "%.6f|%.6f|%.6f|%d|%.6f",
            slope_per_year,   // pendiente anual
            intercept,        // intercepto (a escala mensual)
            r2,
            n,
            ybar              // media mensual
        ));
        ctx.write(dep, outVal);
    }
}
