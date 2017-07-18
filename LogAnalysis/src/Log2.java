import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 7/17/17.
 */
public class Log2 {
    public static class Log2Mapper extends Mapper<Object, Text, Text, IntWritable> {
        Text keyWord = new Text();
        IntWritable valueWord = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\s+");

            // get time
            int hour = Integer.parseInt(tokens[1].split(":")[1]);
            String time1 = hour + "";
            String time2 = (hour + 1) % 24 + "";
            String time = time1 + ":00-" + time2 + ":00";
            // get ip
            String ip = tokens[0];

            // ip#time, 1
            String kString = ip + "#" + time;
            keyWord.set(kString);
            context.write(keyWord, valueWord);

            // ip, 1
            keyWord.set(ip);
            context.write(keyWord, valueWord);
        }
    }

    public static class Log2Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            IntWritable valueWord = new IntWritable(sum);
            context.write(key, valueWord);
        }
    }


    public static class Log2Reducer extends Reducer<Text, IntWritable, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            String[] tokens = key.toString().split("#");
            String ip = tokens[0];
            keyWord.set(ip);

            if (tokens.length == 1) {
                valueWord.set(ip + ":" + sum);
            } else {
                String time = tokens[1];
                valueWord.set(time + " " + ip + ":" + sum);
            }
            context.write(keyWord, valueWord);
        }
    }
}
