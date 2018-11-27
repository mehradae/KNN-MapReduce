package edu.ucr.cs.cs226.mamin021;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



import java.time.LocalDate;
import java.time.LocalTime;



/**
 * Hello world!
 *
 */
public class KNN {

    public static void main(String[] args) throws Exception {


        System.out.print("We are starting the app\n");
        if(args.length!=4){
            System.out.print((char)27 + "[31m" +"This is not a correct input format!\n"+(char)27 + "[32m"+
                    "Correct format : <"+(char)27 + "[1m" + "input path"+(char)27 +"[0m"+(char)27 + "[32m" + "> <"
                    + (char)27 +"[1m" + "Xpoint in query"+(char)27 + "[0m"+(char)27 + "[32m"+"> <"+
                    (char)27 + "[1m"+"Ypoint in query"+(char)27 + "[0m"+(char)27 + "[32m"+"> <"
                    +(char)27 + "[1m"+"K factor"+(char)27 + "[0m"+(char)27 + "[32m"+">"+(char)27 + "[0m");
            return;
        }
        Configuration conf = new Configuration();

        // getting query points
        conf.set("Xpoint",args[1]);
        conf.set("Ypoint",args[2]);

        //getting K number from input
        conf.setInt("K",Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "MapReduceKNN");
        job.setJarByClass(KNN.class);



        // Setup MapReduce job
        job.setMapperClass(KnnMapper.class);
        job.setReducerClass(KnnReducer.class);
        job.setNumReduceTasks(1); // Only one reducer is enough

        // Specify key / value
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        // Input (the data file) and Output (the resulting classification)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        LocalTime lt = LocalTime.now();
        LocalDate ld = LocalDate.now();
        Path pt = new Path("KNN_results_"+ld+"_"+lt.getHour()+"-"+
                lt.getMinute()+"-"+lt.getSecond());

        //FileOutputFormat.setOutputPath(job, pt);

        // Execute job and return status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}