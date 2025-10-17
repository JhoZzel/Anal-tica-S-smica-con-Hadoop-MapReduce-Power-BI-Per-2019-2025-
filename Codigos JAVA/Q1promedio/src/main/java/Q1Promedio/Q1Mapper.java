/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Q1Promedio;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
/*
public class Q1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
     
        output.collect(new Text(SingleCountryData[7]), one);
    }
    
}*/

/*
// Mapper
public class Q1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text departamento = new Text();
    private DoubleWritable valor = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields = line.toString().split(",");

        // Evita encabezado o filas incompletas
        if (fields.length < 8 || line.toString().contains("ACEL_MAX_VERTICAL")) return;

        String dep = fields[2].trim();
        double acelVert = Double.parseDouble(fields[10].trim());
        departamento.set(dep);
        valor.set(acelVert);
        context.write(departamento, valor);
    }
}*/



public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx) throws IOException, InterruptedException {
        String s = line.toString();
        if (s.isEmpty()) return;
        String[] f = s.split(",");
        if (f.length < 10) return;

        if (f[2].contains("FECHA_CORTE")) return;
      
        try {
            String dep = f[2].trim();
            double v  = Double.parseDouble(f[10].trim());
            double ns = Double.parseDouble(f[11].trim());
            double ew = Double.parseDouble(f[12].trim());
            outKey.set(dep);
            outVal.set(v + "\t" + ns + "\t" + ew + "\t1");
            ctx.write(outKey, outVal);
        } catch (Exception ignore) {}
    }
}
