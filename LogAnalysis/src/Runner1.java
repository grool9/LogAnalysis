import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 7/12/17.
 */
public class Runner1 {
    public static void main(String[] args) throws Exception {
        // task1
        Configuration jobconf1 = new Configuration();
        Job job1 = Job.getInstance(jobconf1, "Log1");
        job1.setJarByClass(Log1.class);
        job1.setMapperClass(Log1.Log1Mapper.class);
        job1.setCombinerClass(Log1.Log1Combiner.class);
        job1.setReducerClass(Log1.Log1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        // output
        job1.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // task3
        Configuration jobconf3 = new Configuration();
        Job job3 = Job.getInstance(jobconf3, "Log3");
        job3.setJarByClass(Log3.class);
        job3.setMapperClass(Log3.Log3Mapper.class);
        job3.setCombinerClass(Log3.Log3Combiner.class);
        job3.setReducerClass(Log3.Log3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[0]));
        // output
        job3.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }
}
