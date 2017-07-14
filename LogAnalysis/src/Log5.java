import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 7/13/17.
 */
public class Log5 {
    public static class Log5Mapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.log"))[0];
            int len = fileName.length();
            fileName = fileName.substring(len-2);

            Text keyWord = new Text();
            IntWritable valueCount = new IntWritable(1);

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
            String kString = time + " " + url + "#" + fileName ;
            keyWord.set(kString);
            context.write(keyWord, valueCount);
        }
    }

    public static class Log5Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            IntWritable valueWord = new IntWritable(sum);
            context.write(key, valueWord);
        }
    }

    public static class Log5Partitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0]; // time url
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class Log5Reducer extends Reducer<Text, IntWritable, Text, Text> {
        Text keyWord = new Text();
        Text valueWord = new Text();
        String fileName = null;
        String time = null;
        String url = null;
        static Text currentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] tokens = key.toString().split("#");
            keyWord.set(tokens[0]); // time url

            fileName = tokens[1]; // fileName
            time = tokens[0].split(" ")[0];
            url = tokens[0].split(" ")[1];
            if(!url.equals("null"))url = url.substring(1).replace("/", "-");

            int sum = 0;
            for (IntWritable val:values) {
                sum += val.get();
            }
            valueWord.set(fileName + ":" + sum);

            if(!currentItem.equals(keyWord) && !currentItem.equals(" ")) {

                StringBuilder out = new StringBuilder(time);
                for(String p: postingList) {
                    out.append(" ");
                    out.append(p);
                }
                context.write(new Text(url), new Text(out.toString()));
                postingList = new ArrayList<String>();
            }

            currentItem = new Text(keyWord);
            postingList.add(valueWord.toString());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder(time);
            for(String p: postingList) {
                out.append(" ");
                out.append(p);
            }

            context.write(new Text(url), new Text(out.toString()));
        }
    }
}
