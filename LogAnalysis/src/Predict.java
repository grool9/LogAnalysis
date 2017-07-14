import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 7/13/17.
 */
public class Predict {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.txt"))[0];

            int day = Integer.parseInt(fileName.substring(0,2));
            if(day == 22)return;

            String line = value.toString();
            String[] tokens = line.split("\\s+");
            if(tokens.length < 2) return;

            String time = tokens[0];
            String[] t = tokens[1].split(":");
            String url = t[0];
            int number = Integer.parseInt(t[1]);

            Text keyWord = new Text();
            Text valueWord = new Text("1");

            String kString = time + "#" + url;
            String vString = "" + (number * (day - 7));

            keyWord.set(kString);
            valueWord.set(vString);
            context.write(keyWord, valueWord);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            sum /= 105;

            Text keyWord = new Text();
            Text valueWord = new Text();

            String[] tokens = key.toString().split("#");
            String time = tokens[0];
            String url = tokens[1];
            String vString = time + " " + url + ":" + sum;

            String kString = url;
            if(url.charAt(0) == '/')kString = kString.substring(1);
            kString = kString.replace("/", "-");

            keyWord.set(kString);
            valueWord.set(vString);

            context.write(keyWord, valueWord);
        }
    }
}
