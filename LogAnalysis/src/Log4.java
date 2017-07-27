import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hadoop on 7/18/17.
 */
public class Log4 {
    public static class Log4Mapper extends Mapper<Object, Text, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();

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

            // get url
            String url = tokens[4];
            if(url.charAt(url.length()-1) == '/')url = url.substring(0, url.length()-1);

            // get response time
            String rt = tokens[tokens.length - 1];

            // ip#time, response time
            String kString = url + "#" + time;
            keyWord.set(kString);
            valueWord.set(rt + "#1");
            context.write(keyWord, valueWord);

            // ip, response time
            keyWord.set(url);
            context.write(keyWord, valueWord);
        }
    }

    public static class Log4Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int number = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString().split("#")[0]);
                number += Integer.parseInt(value.toString().split("#")[1]);
            }
            Text valueWord = new Text(sum + "#" + number);
            context.write(key, valueWord);
        }
    }


    public static class Log4Reducer extends Reducer<Text, Text, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int number = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString().split("#")[0]);
                number += Integer.parseInt(value.toString().split("#")[1]);
            }
            double ave = (double)sum / number;

            String[] tokens = key.toString().split("#");
            String url = tokens[0];

            String kString = url;
            if(kString.charAt(0) == '/') {
                kString = kString.substring(1);
                kString = kString.replace("/", "-");
            }
            keyWord.set(kString);

            if (tokens.length == 1) {
                valueWord.set(url + ":" + ave);
            } else {
                String time = tokens[1];
                valueWord.set(time + " " + url + ":" + ave);
            }
            context.write(keyWord, valueWord);
        }
    }
}
