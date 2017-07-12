/**
 * Created by hadoop on 2017/7/10.
 */
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Log1 {
    public static class Log1Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text keyWord = new Text();
            Text valueWord = new Text();
            Text valueCount = new Text("1");

            String line = value.toString();
            String[] tokens = line.split("\\s+");
            int number = tokens.length;

            // get time and state
            int hour = Integer.parseInt(tokens[1].split(":")[1]);
            String time1 = hour + "";
            String time2 = (hour + 1) % 24 + "";
            String time = time1 + ":00-" + time2 + ":00";
            String state = tokens[number-3];

            // each hour
            String kString = ";" + time;
            String vString = state + ":" + valueCount.toString();
            keyWord.set(kString);
            valueWord.set(vString);
            context.write(keyWord, valueWord);

            // all time
            keyWord.set(state);
            context.write(keyWord, valueCount);
        }
    }

    public static class Log1Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int sum1 = 0;//200
            int sum2 = 0;//404
            int sum3 = 0;//500
            boolean isEachHour = false;

            Text valueWord = new Text();
            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                if(tokens.length == 1) {
                    sum += 1;
                }
                else{
                    isEachHour = true;
                    switch (tokens[0]) {
                        case "200": sum1 += 1; break;
                        case "404": sum2 += 1; break;
                        case "500": sum3 += 1; break;
                    }
                }
            }
            if(!isEachHour) {
                valueWord.set("" + sum);
                context.write(key, valueWord);
            }
            else {
                valueWord.set("200:" + sum1);
                context.write(key, valueWord);

                valueWord.set("404:" + sum2);
                context.write(key, valueWord);

                valueWord.set("500:" + sum3);
                context.write(key, valueWord);
            }
        }
    }

    public static class Log1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int sum1 = 0;//200
            int sum2 = 0;//404
            int sum3 = 0;//500
            boolean isEachHour = false;

            Text valueWord = new Text("1");
            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                if(tokens.length == 1) {
                    isEachHour = false;
                    sum += Integer.parseInt(value.toString());
                }
                else{
                    isEachHour = true;
                    switch (tokens[0]) {
                        case "200":sum1 += Integer.parseInt(tokens[1]); break;
                        case "404":sum2 += Integer.parseInt(tokens[1]); break;
                        case "500":sum3 += Integer.parseInt(tokens[1]); break;
                    }
                }
            }
            if(!isEachHour) {
                key.set(key.toString() + ":" + sum);
                context.write(key, valueWord);
            }
            else {
                String[] t = key.toString().split(";");
                String kString = t[1] + " 200:" + sum1 + " 404:"
                        + sum2 + " 500:" + sum3;
                key.set(kString);
                context.write(key, valueWord);
            }
        }
    }
}
