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
            Text valueWord = new Text();
            Text valueCount = new Text("1");

            String line = value.toString();
            String[] tokens = line.split("\\s+");

            // get time and url
            String[] timeToken = tokens[1].split(":");

            String url = tokens[4];
            if(url.equals("null"))return; // remove ip - null

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
            String kString = "{" + time ;
            String vString = url + ":" + valueCount.toString();
            keyWord.set(kString);
            valueWord.set(vString);
            context.write(keyWord, valueWord);

            // all time
            keyWord.set(url);
            context.write(keyWord, valueCount);
        }
    }

    public static class Log3Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Hashtable<String, Integer> table = new Hashtable<String, Integer>();
            int sum = 0;
            boolean isEachSecond = false;

            Text valueWord = new Text();
            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                if(tokens.length == 1) {
                    isEachSecond = false;
                    sum += 1;
                }
                else{
                    isEachSecond = true;
                    String k = tokens[0];
                    int v = 1;
                    if(table.containsKey(k)) {
                        v = table.get(k) + v;
                        table.remove(k);
                        table.put(k, v);
                    }
                    else table.put(k, v);
                }
            }
            if(!isEachSecond) {
                valueWord.set("" + sum);
                context.write(key, valueWord);
            }
            else {
                for(String k: table.keySet()) {
                    String vString = k + ":" + table.get(k);
                    valueWord.set(vString);
                    context.write(key, valueWord);
                }
            }
        }
    }

    public static class Log3Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Hashtable<String, Integer> table = new Hashtable<String, Integer>();
            int sum = 0;
            boolean isEachSecond = false;

            Text valueWord = new Text("our-category-query");
            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                if(tokens.length == 1) {
                    isEachSecond = false;
                    sum += Integer.parseInt(value.toString());
                }
                else{
                    isEachSecond = true;
                    String k = tokens[0];
                    int v = Integer.parseInt(tokens[1]);
                    if(table.containsKey(k)) {
                        v = table.get(k) + v;
                        table.remove(k);
                        table.put(k, v);
                    }
                    else table.put(k, v);
                }
            }
            if(!isEachSecond) {
                key.set(key.toString() + ":" + sum);
                context.write(key, valueWord);
            }
            else {
                String[] t = key.toString().split("\\{");
                String kString = t[1];
                for(String k: table.keySet()) {
                    kString = kString + " " + k + ":" + table.get(k);
                }
                key.set(kString);
                context.write(key, valueWord);
            }
        }
    }
}