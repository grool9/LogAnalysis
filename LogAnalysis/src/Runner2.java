import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by hadoop on 7/13/17.
 */
public class Runner2 {
    public static void main(String[] args) throws Exception {
        // task5
        Configuration jobconf5 = new Configuration();
        Job job5 = Job.getInstance(jobconf5, "Log1");
        job5.setJarByClass(Log5.class);
        job5.setMapperClass(Log5.Log5Mapper.class);
        job5.setCombinerClass(Log5.Log5Combiner.class);
        job5.setReducerClass(Log5.Log5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        // input
        FileInputFormat.setInputDirRecursive(job5, true);
        FileInputFormat.addInputPath(job5, new Path(args[0]));
        // output
        job5.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job5, new Path(args[1]));
        job5.waitForCompletion(true);
    }
}
