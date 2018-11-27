package edu.ucr.cs.cs226.mamin021;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class KnnReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

    TreeMap<Text, DoubleWritable> KnnMap = new TreeMap<>();


    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int K = context.getConfiguration().getInt("K", 1);
        for (Text val : values) {


            KnnMap.put(val, key);
            //Sorting

            if(K>0)
            context.write(NullWritable.get(), new Text(ResultFormatter(KnnMap)));
            K--;
            context.getConfiguration().setInt("K",K);
        }


    }

    private static String ResultFormatter(TreeMap<Text, DoubleWritable> knnMap) {

        String result = "";
        for (Map.Entry<Text, DoubleWritable> entry : knnMap.entrySet()) {
            result += entry.getKey().toString() + "\t" + entry.getValue() + "\n";
        }
        return result;
    }
}
