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
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Collections;

public class Q2Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text();
    private boolean headerWritten = false;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        if (!headerWritten) {
            ctx.write(new Text("LIMA"),
                      new Text("Med_ACEL_VERT\tMed_ACEL_NS\tMed_ACEL_EO"));
            headerWritten = true;
        }
        
        
        ArrayList<Double> listV = new ArrayList<>();
        ArrayList<Double> listNS = new ArrayList<>();
        ArrayList<Double> listEW = new ArrayList<>();

        for (Text t : values) {
            String[] p = t.toString().split("\t");
            listV.add(Double.parseDouble(p[0]));
            listNS.add(Double.parseDouble(p[1]));
            listEW.add(Double.parseDouble(p[2]));
        }
        
        Collections.sort(listV);
        Collections.sort(listNS);
        Collections.sort(listEW);
        
        int n = listV.size();
        
        double mediana_V = listV.get(n / 2);
        double mediana_NS = listNS.get(n / 2);
        double mediana_EW = listEW.get(n / 2);
        
       
        outVal.set(String.format("%.6f\t%.6f\t%.6f", mediana_V, mediana_NS, mediana_EW));
        ctx.write(key, outVal);
    }
}