import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 7/13/17.
 */
public class test {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text keyWord = new Text();
            Text valueCount = new Text("1");

            String line = value.toString();

            String[] tokens = line.split("\\s+");
            if(tokens.length < 9){
                context.write(value, new Text());
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // task5
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "test");
        job.setJarByClass(test.class);
        job.setMapperClass(test.MyMapper.class);
        job.setReducerClass(test.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // input
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // output
//        job5.setOutputFormatClass(MultipleOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
