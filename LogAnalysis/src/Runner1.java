import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.net.URI;

/**
 * Created by hadoop on 7/12/17.
 */
public class Runner1 {
    public static void main(String[] args) throws Exception {
        Path in = new Path(args[0]);
        Path out1 = new Path(args[1]);
        Path out2 = new Path(args[2]);
        Path out3 = new Path(args[3]);
        Path out4 = new Path(args[4]);
        FileSystem fileSystem = FileSystem.get(new URI(in.toString()), new Configuration());
        if (fileSystem.exists(out1)) {
            fileSystem.delete(out1, true);
        }
        if (fileSystem.exists(out2)) {
            fileSystem.delete(out2, true);
        }
        if (fileSystem.exists(out3)) {
            fileSystem.delete(out3, true);
        }
        if (fileSystem.exists(out4)) {
            fileSystem.delete(out4, true);
        }

        // task1
        Configuration jobconf1 = new Configuration();
        Job job1 = Job.getInstance(jobconf1, "Log1");
        job1.setJarByClass(Log1.class);
        job1.setMapperClass(Log1.Log1Mapper.class);
        job1.setCombinerClass(Log1.Log1Combiner.class);
        job1.setReducerClass(Log1.Log1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, in);
        // output
        job1.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, out1);
        job1.waitForCompletion(true);

        // task2
        Configuration jobconf2 = new Configuration();
        Job job2 = Job.getInstance(jobconf2, "Log2");
        job2.setJarByClass(Log2.class);
        job2.setMapperClass(Log2.Log2Mapper.class);
        job2.setCombinerClass(Log2.Log2Combiner.class);
        job2.setReducerClass(Log2.Log2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, in);
        // output
        job2.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, out2);
        job2.waitForCompletion(true);

        // task3
        Configuration jobconf3 = new Configuration();
        Job job3 = Job.getInstance(jobconf3, "Log3");
        job3.setJarByClass(Log3.class);
        job3.setMapperClass(Log3.Log3Mapper.class);
        job3.setCombinerClass(Log3.Log3Combiner.class);
        job3.setReducerClass(Log3.Log3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, in);
        // output
        job3.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job3, out3);
        job3.waitForCompletion(true);

        // task4
        Configuration jobconf4 = new Configuration();
        Job job4 = Job.getInstance(jobconf4, "Log4");
        job4.setJarByClass(Log4.class);
        job4.setMapperClass(Log4.Log4Mapper.class);
        job4.setCombinerClass(Log4.Log4Combiner.class);
        job4.setReducerClass(Log4.Log4Reducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, in);
        // output
        job4.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job4, out4);
        job4.waitForCompletion(true);
    }
}
