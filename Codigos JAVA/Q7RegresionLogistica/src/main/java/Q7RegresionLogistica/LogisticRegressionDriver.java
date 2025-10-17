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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;
import java.util.Arrays;

public class LogisticRegressionDriver {

    public static final int NUM_FEATURES = 3; // 2 predictores (Lat, Lon) + 1 intercepto
    public static final int MAX_ITERATIONS = 20;
    public static final double LEARNING_RATE = 0.05;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: LogisticRegressionDriver <directorio_entrada> <directorio_base_salida>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        // Inicializar los coeficientes del modelo (betas) con ceros
        double[] beta = new double[NUM_FEATURES];
        Arrays.fill(beta, 0.0);

        System.out.println("Iniciando entrenamiento con Descenso de Gradiente...");
        System.out.println("Coeficientes iniciales (beta): " + Arrays.toString(beta));

        for (int i = 0; i < MAX_ITERATIONS; i++) {
            System.out.println("\n===== ITERACIÓN " + (i + 1) + " =====");

            // Guardar los betas actuales en un archivo para que el Mapper los lea
            Path betaPath = new Path(args[1] + "/betas/beta-" + i + ".txt");
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(betaPath, true)))) {
                for (int j = 0; j < beta.length; j++) {
                    writer.write(String.valueOf(beta[j]));
                    if (j < beta.length - 1) writer.write(",");
                }
            }

            // Configurar el trabajo MapReduce
            Job job = Job.getInstance(conf, "Calculo de Gradiente - Iteracion " + (i + 1));
            job.setJarByClass(LogisticRegressionDriver.class);
            job.setMapperClass(GradientMapper.class);
            job.setReducerClass(GradientReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // Pasar la ruta del archivo de betas al Mapper
            job.getConfiguration().set("beta.path", betaPath.toString());

            Path outputPath = new Path(args[1] + "/iteration-" + (i + 1));
            // Eliminar directorio de salida si ya existe
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, outputPath);

            if (!job.waitForCompletion(true)) {
                System.err.println("La Iteración " + (i + 1) + " falló.");
                System.exit(1);
            }

            // Leer el gradiente calculado por el Reducer
            Path gradientPath = new Path(outputPath, "part-r-00000");
            double[] gradient = new double[NUM_FEATURES];
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(gradientPath)))) {
                String line = reader.readLine();
                // La salida del reducer es "0\tgrad0,grad1,grad2"
                String[] parts = line.split("\t")[1].split(",");
                for (int j = 0; j < parts.length; j++) {
                    gradient[j] = Double.parseDouble(parts[j]);
                }
            }

            System.out.println("Gradiente total calculado: " + Arrays.toString(gradient));

            // Actualizar los coeficientes (betas) usando el Descenso de Gradiente
            for (int j = 0; j < beta.length; j++) {
                beta[j] = beta[j] - LEARNING_RATE * gradient[j];
            }

            System.out.println("Coeficientes actualizados (beta): " + Arrays.toString(beta));
        }

        System.out.println("\n===== ENTRENAMIENTO FINALIZADO =====");
        System.out.println("Coeficientes finales del modelo: " + Arrays.toString(beta));
    }
}
