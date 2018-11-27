package edu.ucr.cs.cs226.mamin021;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class KnnMapper extends Mapper<Object, Text, DoubleWritable, Text> {



    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");

        double x = Double.parseDouble(parts[1]);
        double y = Double.parseDouble(parts[2]);

        //Get the query points
        double XPoint= Double.parseDouble(context.getConfiguration().get("Xpoint"));
        double YPoint = Double.parseDouble(context.getConfiguration().get("Ypoint")) ;

        //Calculating Distance
        Double distance = Math.sqrt((XPoint - x) * (XPoint - x) + (YPoint - y) * (YPoint - y));
        context.write(new DoubleWritable(distance),value);

    }

}
