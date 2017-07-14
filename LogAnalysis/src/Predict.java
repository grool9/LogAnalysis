import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * Created by hadoop on 7/13/17.
 */
public class Predict {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.txt"))[0];
            String line = value.toString();

            SimpleRegression simpleRegression = new SimpleRegression(true);
            String[] tokens = line.split("\\s+");
            int number = tokens.length;
            if(!tokens[number - 1].split(":")[0].equals("22"))return;

            double[][] array = new double[number - 1][2];
            for(int i = 1; i < number - 1 ; i++) {
                String[] t = tokens[i].split(":");
                array[i-1][0] = Double.parseDouble(t[0]);
                array[i-1][1] = Double.parseDouble(t[1]);
            }

            simpleRegression.addData(array);
            int predict = new Double(simpleRegression.predict(22)).intValue();
            String real = tokens[tokens.length-1].split(":")[1];

            Text keyWord = new Text(fileName);
            Text valueWord = new Text(predict + " " + real);
            context.write(keyWord, valueWord);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(key, value);
            }
        }
    }
}
