/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Q2Mediana;

/**
 *
 * @author jhozzel
 */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx) throws IOException, InterruptedException {
        String s = line.toString();

        if (s.contains("FECHA_CORTE")) return;
        
        String[] f = s.split(",");
        String dep = f[2].trim();
        
        if (!"LIMA".equalsIgnoreCase(dep)) return;
        
        String date = f[5].trim();
        String year = date.substring(0,4);
        
        double v  = Double.parseDouble(f[10].trim());
        double ns = Double.parseDouble(f[11].trim());
        double ew = Double.parseDouble(f[12].trim());
        outKey.set(year);
        outVal.set(v + "\t" + ns + "\t" + ew);
        ctx.write(outKey, outVal);
    }
}
