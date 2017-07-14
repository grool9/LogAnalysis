import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.rmi.runtime.Log;

import java.net.URI;

/**
 * Created by hadoop on 7/13/17.
 */
public class Runner2 {
    public static void main(String[] args) throws Exception {
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileSystem fileSystem = FileSystem.get(new URI(in.toString()), new Configuration());
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        // task5
        Configuration jobconf5 = new Configuration();
        Job job5 = Job.getInstance(jobconf5, "Log5");
        job5.setJarByClass(Log5.class);
        job5.setMapperClass(Log5.Log5Mapper.class);
        job5.setCombinerClass(Log5.Log5Combiner.class);
        job5.setPartitionerClass(Log5.Log5Partitioner.class);
        job5.setReducerClass(Log5.Log5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);

        // input
        FileInputFormat.setInputDirRecursive(job5, true);
        FileInputFormat.addInputPath(job5, in);
        // output
        job5.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job5, out);
        job5.waitForCompletion(true);

        // predict
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Predict");
//        job.setJarByClass(Predict.class);
//        job.setMapperClass(Predict.MyMapper.class);
//        job.setReducerClass(Predict.MyReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        // input
//        FileInputFormat.setInputDirRecursive(job, true);
//        FileInputFormat.addInputPath(job, new Path(outpath));
//        // output
//        job.setOutputFormatClass(MultipleOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.waitForCompletion(true);
    }
}