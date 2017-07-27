import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.net.URI;

/**
 * Created by hadoop on 7/13/17.
 */
public class Runner2 {
    public static void main(String[] args) throws Exception {
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        Path out1 = new Path("tmp1");
        Path out2 = new Path("tmp2");
        Path out3 = new Path("tmp3");

        FileSystem fileSystem = FileSystem.get(new URI(in.toString()), new Configuration());


        // task5  2 jobs
        Configuration conf1 = new Configuration();
        Job j1 = Job.getInstance(conf1, "DataPre1");
        j1.setJarByClass(DataPre1.class);
        j1.setMapperClass(DataPre1.MyMapper.class);
        j1.setCombinerClass(DataPre1.MyCombiner.class);
        j1.setReducerClass(DataPre1.MyReducer.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputDirRecursive(j1, true);
        FileInputFormat.addInputPath(j1, in);
        // output
        j1.setOutputFormatClass(MultipleOutputFormat.class);
        if (fileSystem.exists(out1)) {
            fileSystem.delete(out1, true);
        }
        FileOutputFormat.setOutputPath(j1, out1);
        j1.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job j2 = Job.getInstance(conf2, "DataPre2");
        j2.setJarByClass(DataPre2.class);
        j2.setMapperClass(DataPre2.MyMapper.class);
        j2.setReducerClass(DataPre2.MyReducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(j2, true);
        FileInputFormat.addInputPath(j2, out1);
        // output
        j2.setOutputFormatClass(MultipleOutputFormat.class);
        if (fileSystem.exists(out2)) {
            fileSystem.delete(out2, true);
        }
        FileOutputFormat.setOutputPath(j2, out2);
        j2.waitForCompletion(true);


        // predict
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Predict");
        job.setJarByClass(PredictGlobal.class);
        job.setMapperClass(PredictGlobal.MyMapper.class);
        job.setReducerClass(PredictGlobal.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // input
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, out2);
        // output
        job.setOutputFormatClass(MultipleOutputFormat.class);
        if (fileSystem.exists(out3)) {
            fileSystem.delete(out3, true);
        }
        FileOutputFormat.setOutputPath(job, out3);
        job.waitForCompletion(true);

        // validate
        Configuration conf3 = new Configuration();
        Job job2 = Job.getInstance(conf3, "Validate");
        job2.setJarByClass(Validate.class);
        job2.setMapperClass(Validate.MyMapper.class);
        job2.setReducerClass(Validate.MyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // input
        FileInputFormat.setInputDirRecursive(job2, true);
        FileInputFormat.addInputPath(job2, out3);
        // output
        job2.setOutputFormatClass(MultipleOutputFormat.class);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job2, out);
        job2.waitForCompletion(true);
    }
}
