/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Q7RegresionLogistica;

/**
 *
 * @author jhozzel
 */
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GradientReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private static final int NUM_FEATURES = 3;

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double[] totalGradient = new double[NUM_FEATURES];

        // Sumar todos los gradientes parciales recibidos de los mappers
        for (Text val : values) {
            String[] partialGradientStr = val.toString().split(",");
            for (int i = 0; i < partialGradientStr.length; i++) {
                totalGradient[i] += Double.parseDouble(partialGradientStr[i]);
            }
        }

        // Emitir el gradiente total
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < totalGradient.length; i++) {
            sb.append(totalGradient[i]);
            if (i < totalGradient.length - 1) sb.append(",");
        }

        context.write(key, new Text(sb.toString()));
    }
}
