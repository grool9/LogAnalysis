import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 7/26/17.
 */
public class DataPre1 {
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.log"))[0];
            int len = fileName.length();
            fileName = fileName.substring(len-2);

            Text keyWord = new Text();
            IntWritable valueCount = new IntWritable(1);

            String line = value.toString();
            String[] tokens = line.split("\\s+");
            if(tokens.length < 2) return;

            // get url
            String url = tokens[4];
            if(url.charAt(url.length()-1) == '/')url = url.substring(0, url.length()-1);
            if(!url.equals("null"))url = url.substring(1).replace("/", "-");

            String[] timeToken = tokens[1].split(":");
            // get time
            int hour = Integer.parseInt(timeToken[1]);
            String time1 = hour + "";
            String time2 = (hour + 1) % 24 + "";
            String time = time1 + ":00-" + time2 + ":00";

            // time#url#fileName
            String kString = time + "#" + url + "#" + fileName ;
            keyWord.set(kString);

            context.write(keyWord, valueCount);
        }
    }

    public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            IntWritable valueWord = new IntWritable(sum);
            context.write(key, valueWord);
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            String[] tokens = key.toString().split("#");
            String time = tokens[0];
            String url = tokens[1];
            String date = tokens[2];

            String kString = url + "#" + date;
            String vString = time + " " + sum;
            context.write(new Text(kString), new Text(vString));
        }

    }
}
