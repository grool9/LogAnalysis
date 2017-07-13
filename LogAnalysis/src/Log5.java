import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 7/13/17.
 */
public class Log5 {
    public static class Log5Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.log"))[0];
            int len = fileName.length();
            fileName = fileName.substring(len-2);

            Text keyWord = new Text();
            Text valueCount = new Text("1");

            String line = value.toString();

            String[] tokens = line.split("\\s+");
            if(tokens.length < 2) return;

            // get url
            String url = tokens[4];

            String[] timeToken = tokens[1].split(":");
            // get time
            int hour = Integer.parseInt(timeToken[1]);
            String time1 = hour + "";
            String time2 = (hour + 1) % 24 + "";
            String time = time1 + ":00-" + time2 + ":00";

            // each hour
            String kString = "{" + fileName + "#" + time + "#" + url ;
            keyWord.set(kString);
            context.write(keyWord, valueCount);

            // all time
            kString = fileName + "#" + url;
            keyWord.set(kString);
            context.write(keyWord, valueCount);
        }
    }

    public static class Log5Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            Text valueWord = new Text(""+sum);
            context.write(key, valueWord);
        }
    }

    public static class Log5Reducer extends Reducer<Text, Text, Text, Text> {
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
            if(tokens.length == 2) {
                String filename = tokens[0];
                String url = tokens[1];

                if(url.charAt(0) == '/')kString = url.substring(1);
                kString = filename + "/" + kString.replace("/", "-");

                vString = url + ":" + sum;
            }
            else {
                String filename = tokens[0].split("\\{")[1];
                String time = tokens[1];
                String url = tokens[2];

                if(url.charAt(0) == '/')kString = url.substring(1);
                kString = filename + "/" + kString.replace("/", "-");

                vString = time + " " + url + ":" + sum;
            }

            keyWord.set(kString);
            valueWord.set(vString);

            context.write(keyWord, valueWord);
        }
    }
}
