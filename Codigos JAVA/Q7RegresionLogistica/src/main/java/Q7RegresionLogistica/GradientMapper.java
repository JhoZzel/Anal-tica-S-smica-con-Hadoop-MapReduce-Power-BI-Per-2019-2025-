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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class GradientMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private double[] beta;
    private double[] partialGradient;
    private static final int NUM_FEATURES = 3; // Lat, Lon, Intercepto
    private static final double UMBRAL_PELIGRO = 0.2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Inicializar el gradiente parcial para este mapper
        partialGradient = new double[NUM_FEATURES];
        beta = new double[NUM_FEATURES];

        // Leer los coeficientes (beta) actuales desde el archivo en HDFS
        Configuration conf = context.getConfiguration();
        Path betaPath = new Path(conf.get("beta.path"));
        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(betaPath)))) {
            String line = reader.readLine();
            String[] beta_str = line.split(",");
            for (int i = 0; i < beta_str.length; i++) {
                beta[i] = Double.parseDouble(beta_str[i]);
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignorar la cabecera del CSV
        if (key.get() == 0) {
            return;
        }

        String[] fields = value.toString().split(",");

        try {
            // Columnas de interés (ajusta los índices si tu CSV es diferente)
            double latEstacion = Double.parseDouble(fields[8]);      // LAT_ESTACION
            double lonEstacion = Double.parseDouble(fields[9]);      // LON_ESTACION
            double acelNorteSur = Double.parseDouble(fields[11]);   // ACEL_MAX_NORTE-SUR
            double acelEsteOeste = Double.parseDouble(fields[12]);  // ACEL_MAX_ESTE-OESTE

            // --- PASO 1: Crear las variables ---
            double pgaHorizontal = Math.max(Math.abs(acelNorteSur), Math.abs(acelEsteOeste));
            int peligroEstructural = (pgaHorizontal > UMBRAL_PELIGRO) ? 1 : 0;

            // --- PASO 2: Calcular la predicción ---
            // Features: X = [1, latEstacion, lonEstacion] (1 es para el intercepto)
            double z = beta[0] * 1.0 + beta[1] * latEstacion + beta[2] * lonEstacion;
            // Función Sigmoide para obtener probabilidad
            double prediccion = 1.0 / (1.0 + Math.exp(-z));

            // --- PASO 3: Calcular el gradiente para esta fila ---
            double error = prediccion - peligroEstructural;

            partialGradient[0] += error * 1.0; // Gradiente para el intercepto
            partialGradient[1] += error * latEstacion;
            partialGradient[2] += error * lonEstacion;

        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            // Ignorar líneas malformadas
            System.err.println("Saltando línea malformada: " + value.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Al final, emitir el gradiente parcial acumulado por este mapper
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partialGradient.length; i++) {
            sb.append(partialGradient[i]);
            if (i < partialGradient.length - 1) sb.append(",");
        }
        context.write(new IntWritable(0), new Text(sb.toString()));
    }
}
