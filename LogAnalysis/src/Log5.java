import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 7/27/17.
 */
public class Log5 {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // fileName --> get time
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String temp = (fileSplit.getPath().getName().split("\\.txt"))[0];
            String[] t = temp.split("-");
            String time = t[0] + ":00-" + t[1] + ":00";

            String line = value.toString();
            String[] tokens = line.split(" ");
            String url = tokens[0];
            String predict = tokens[1];

            keyWord.set(url);
            valueWord.set(time + " " + url + ":" + predict);
            context.write(keyWord, valueWord);
        }
    }


    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text v : values){
                context.write(key, v);
            }
        }
    }
}
