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
    public static class Log3Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text keyWord = new Text();

            Text valueCount = new Text("1");

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

            String time1 = hour + ":";
            if(minute < 10) {
                time1 += "0";
            }
            time1 += minute + ":";
            if(second < 10) {
                time1 += "0";
            }
            time1 += second;

            String time2 = hour2 + ":";
            if(minute2 < 10) {
                time2 += "0" ;
            }
            time2 += minute2 + ":";
            if(second2 < 10) {
                time2 += "0";
            }
            time2 += second2;

            String time = time1 + "-" + time2;

            // each second
            String kString = "{" + time + "#" + url ;
            keyWord.set(kString);
            context.write(keyWord, valueCount);

            // all time
            keyWord.set(url);
            context.write(keyWord, valueCount);
        }
    }

    public static class Log3Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            Text valueWord = new Text(""+sum);
            context.write(key, valueWord);
        }
    }

    public static class Log3Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            Text keyWord = new Text();
            Text valueWord = new Text();

            String[] tokens = key.toString().split("#");
            String kString = "";
            String vString = "";
            if(tokens.length == 1) {
                String url = key.toString();
                kString = url.substring(1);
                kString = kString.replace("/", "-");
                vString = url + ":" + sum;
            }
            else {
                String time = tokens[0].split("\\{")[1];
                String url = tokens[1];
                kString = url.substring(1);
                kString = kString.replace("/", "-");
                vString = time + " " + url + ":" + sum;
            }

            keyWord.set(kString);
            valueWord.set(vString);

            context.write(keyWord, valueWord);
        }
    }
}