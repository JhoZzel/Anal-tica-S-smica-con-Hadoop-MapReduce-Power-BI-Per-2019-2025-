package Q5TendenciaMensual;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Q5Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: Q5Driver <inputCSV> <outJ1> <outJ2> <outJ3>");
            System.exit(1);
        }
        String in = args[0], out1 = args[1], out2 = args[2], out3 = args[3];

        // ===== Job 1 =====
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Q5-J1 Mensual por DEP");
        job1.setJarByClass(Q5Driver.class);

        job1.setMapperClass(J1Mapper.class);
        job1.setReducerClass(J1Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(in));
        FileOutputFormat.setOutputPath(job1, new Path(out1));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ===== Job 2 =====
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Q5-J2 Regresion por DEP");
        job2.setJarByClass(Q5Driver.class);

        job2.setMapperClass(J2Mapper.class);
        job2.setReducerClass(J2Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(out1));
        FileOutputFormat.setOutputPath(job2, new Path(out2));

        if (!job2.waitForCompletion(true)) System.exit(1);

        // ===== Job 3 =====
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Q5-J3 Ranking por pendiente");
        job3.setJarByClass(Q5Driver.class);

        job3.setMapperClass(J3Mapper.class);
        job3.setReducerClass(J3Reducer.class);

        job3.setSortComparatorClass(J3Reducer.DescDoubleComparator.class);
        job3.setNumReduceTasks(1); // para ranking global ordenado + cabecera única

        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(out2));
        FileOutputFormat.setOutputPath(job3, new Path(out3));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
