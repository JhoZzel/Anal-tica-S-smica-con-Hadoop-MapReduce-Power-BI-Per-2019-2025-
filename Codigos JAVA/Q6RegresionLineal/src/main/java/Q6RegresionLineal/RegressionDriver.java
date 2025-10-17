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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RegressionDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: RegressionDriver <input> <output> [GLOBAL|DEPT]");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("reg.groupby", (args.length >= 3) ? args[2] : "GLOBAL"); // GLOBAL por defecto

        Job job = Job.getInstance(conf, "OLS: VERTICAL ~ NS + EO");
        job.setJarByClass(RegressionDriver.class);

        job.setMapperClass(RegMapper.class);
        job.setCombinerClass(RegCombiner.class);
        job.setReducerClass(RegReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Un solo reducer para emitir cabecera una vez
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // CSV o carpeta
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // carpeta (debe NO existir)

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
