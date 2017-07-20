/**
 * Created by hadoop on 2017/7/10.
 */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Log1 {
    public static class Log1Mapper extends Mapper<Object, Text, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();
        Text valueCount = new Text("1");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\s+");

            // get time
            int hour1 = Integer.parseInt(tokens[1].split(":")[1]);
            int hour2 = (hour1 + 1) % 24;

            String t = "";
            if(hour1 < 10) t = "0";
            String time1 = t + hour1;
            t = "";
            if(hour2 < 10) t = "0";
            String time2 = t + hour2;
            String time = time1 + ":00-" + time2 + ":00";

            // get state
            String state = tokens[tokens.length-3];

            // ;time, state:1
            String kString = ";" + time;
            String vString = state + ":1";
            keyWord.set(kString);
            valueWord.set(vString);
            context.write(keyWord, valueWord);

            // state, 1
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
        Text keyWord = new Text("1");
        Text valueWord = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int sum1 = 0;//200
            int sum2 = 0;//404
            int sum3 = 0;//500
            boolean isEachHour = false;

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
                valueWord.set(key.toString() + ":" + sum);
            }
            else {
                String vString = key.toString().substring(1) + " 200:" + sum1 + " 404:"
                        + sum2 + " 500:" + sum3;
                valueWord.set(vString);
            }
            context.write(keyWord, valueWord);
        }
    }
}