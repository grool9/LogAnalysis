/**
 * Created by hadoop on 7/11/17.
 */
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class Log3 {
    public static class Log3Mapper extends Mapper<Object, Text, Text, IntWritable> {
        Text keyWord = new Text();
        IntWritable valueWord = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\s+");

            // get url
            String url = tokens[4];
            // get time
            String[] timeToken = tokens[1].split(":");

            int hour = Integer.parseInt(timeToken[1]);
            int minute = Integer.parseInt(timeToken[2]);
            int second = Integer.parseInt(timeToken[3]);

            int hour2 = hour;
            int minute2 = minute;
            int second2 = second + 1;

            if(second2 == 60) {
                second2 = 0;
                minute2 += 1;
                if(minute2 == 60) {
                    minute2 = 0;
                    hour2 = (hour2 + 1) % 24;
                }
            }

            // time1
            String time1 = "";
            if(hour < 10) {
                time1 += "0";
            }
            time1 += hour + ":";
            if(minute < 10) {
                time1 += "0";
            }
            time1 += minute + ":";
            if(second < 10) {
                time1 += "0";
            }
            time1 += second;

            // time2
            String time2 = "";
            if(hour2 < 10) {
                time2 += "0";
            }
            time2 += hour2 + ":";
            if(minute2 < 10) {
                time2 += "0" ;
            }
            time2 += minute2 + ":";
            if(second2 < 10) {
                time2 += "0";
            }
            time2 += second2;

            String time = time1 + "-" + time2;

            // url#time, 1
            String kString = url + "#" + time ;
            keyWord.set(kString);
            context.write(keyWord, valueWord);

            // url, 1
            keyWord.set(url);
            context.write(keyWord, valueWord);
        }
    }

    public static class Log3Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            IntWritable valueWord = new IntWritable(sum);
            context.write(key, valueWord);
        }
    }

    public static class Log3Reducer extends Reducer<Text, IntWritable, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            String[] tokens = key.toString().split("#");
            String url = tokens[0];

            String kString = url;
            if(kString.charAt(0) == '/')kString = kString.substring(1);
            kString = kString.replace("/", "-");
            keyWord.set(kString);

            if (tokens.length == 1) {
                valueWord.set(url + ":" + sum);
            } else {
                String time = tokens[1];
                valueWord.set(time + " " + url + ":" + sum);
            }
            context.write(keyWord, valueWord);
        }
    }
}