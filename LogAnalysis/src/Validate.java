import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 7/14/17.
 */
public class Validate {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // fileName --> get time
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String time = (fileSplit.getPath().getName().split("\\.txt"))[0];
            keyWord.set(time);

            String line = value.toString();
            String[] tokens = line.split(" ");
            double predict = Double.parseDouble(tokens[1]);
            double target = Double.parseDouble(tokens[2]);
            double d = predict - target;
            double dd = d * d;
            valueWord.set(dd + "#" + 1);

            context.write(keyWord, valueWord);
        }
    }

    public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double fenzi = 0;
            double fenmu = 0;
            for (Text value : values) {
                String[] tokens = value.toString().split("#");
                fenzi += Double.parseDouble(tokens[0]);
                fenmu += Double.parseDouble(tokens[1]);
            }

            context.write(key, new Text(fenzi + "#" + fenmu));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        static double result = 0;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double fenzi = 0;
            double fenmu = 0;
            for (Text value : values) {
                String[] tokens = value.toString().split("#");
                fenzi += Double.parseDouble(tokens[0]);
                fenmu += Double.parseDouble(tokens[1]);
            }

            result += fenzi / fenmu;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            result /= 24;
            Text keyWord = new Text("result");
            Text valueWord = new Text(result + "");
            context.write(keyWord, valueWord);
        }
    }
}
