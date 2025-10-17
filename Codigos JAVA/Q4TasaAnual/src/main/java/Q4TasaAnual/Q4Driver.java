package Q4TasaAnual;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Q4Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: PipelineDriver <inputCSV> <outJ1> <outJ2> <outJ3>");
            System.exit(1);
        }
        
        
        // orden esperado: <inputCSV> <outJ1> <outJ2> <outJ3>
        String inputCSV = args[0];
        Path outJ1 = new Path(args[1]);
        Path outJ2 = new Path(args[2]);
        Path outJ3 = new Path(args[3]);        
   
        // -------- Job 1: Excedencias por departamento-año --------
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Q4-J1 Excedencias dep-anio (tau fijo en J1Mapper)");
        job1.setJarByClass(Q4Driver.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(J1Mapper.class);
        //job1.setCombinerClass(J1Combiner.class);  // opcional pero recomendado
        job1.setReducerClass(J1Reducer.class);

        job1.setMapOutputKeyClass(Text.class);  // "DEP|YYYY"
        job1.setMapOutputValueClass(Text.class); // "ex|1"
        job1.setOutputKeyClass(Text.class);     // "DEP"
        job1.setOutputValueClass(Text.class);   // "YYYY|exSum|nSum"

        FileInputFormat.addInputPath(job1, new Path(inputCSV));
        FileOutputFormat.setOutputPath(job1, outJ1);
        if (!job1.waitForCompletion(true)) System.exit(1);

        // -------- Job 2: Tasa anual promedio por departamento --------
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job job2 = Job.getInstance(conf2, "Q4-J2 Tasa anual promedio por departamento");
        job2.setJarByClass(Q4Driver.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(J2Mapper.class);
        job2.setReducerClass(J2Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);     // "DEP"
        job2.setOutputValueClass(Text.class);   // "avgRate|years|exTot|nTot"

        FileInputFormat.addInputPath(job2, outJ1);
        FileOutputFormat.setOutputPath(job2, outJ2);
        if (!job2.waitForCompletion(true)) System.exit(1);

        // -------- Job 3: Ranking por periodo de retorno T (asc) --------
        Configuration conf3 = new Configuration();
        conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job job3 = Job.getInstance(conf3, "Q4-J3 Ranking por T ascendente");
        job3.setJarByClass(Q4Driver.class);

        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(J3Mapper.class);
        job3.setReducerClass(J3Reducer.class);
        job3.setNumReduceTasks(1); // orden total

        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
        
        //job3.setMapOutputKeyClass(DoubleWritable.class); // T
        //job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);   // "DEP"
        job3.setOutputValueClass(Text.class); // "T \t avgRate \t years \t exTot \t nTot"

        FileInputFormat.addInputPath(job3, outJ2);
        FileOutputFormat.setOutputPath(job3, outJ3);

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
