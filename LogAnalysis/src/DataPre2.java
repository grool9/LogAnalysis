import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by hadoop on 7/26/17.
 */
public class DataPre2 {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.txt"))[0];

            String url = fileName.split("#")[0];
            String date = fileName.split("#")[1];

            String line = value.toString();
            String time = line.split(" ")[0];
            String number = line.split(" ")[1];

            String kString = time + "#" + url;
            String vString = date + "#" + number;

            context.write(new Text(kString), new Text(vString));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] list = new int[15];
            for (Text value : values) {
                int date = Integer.parseInt(value.toString().split("#")[0]);
                int number = Integer.parseInt(value.toString().split("#")[1]);
                list[date - 8] = number;
            }

            String time = key.toString().split("#")[0];
            String url = key.toString().split("#")[1];

            String kString = url;
            String vString = time;
            for(int i = 0; i < 15; i++) {
                vString += " ";
                vString += i+8;
                vString += ":";
                vString += list[i];
            }
            context.write(new Text(kString), new Text(vString));
        }

    }
}
