import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
//import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.w3c.dom.Attr;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.SMOreg;
import weka.core.*;
import java.util.ArrayList;
import java.util.List;


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


            String[] tokens = line.split("\\s+");
            //int number = tokens.length;
            int len = tokens.length;
            int number = 15;
            int begin = 8;
            int winsize = 5;
            //if(!tokens[number - 1].split(":")[0].equals("22"))return;

            // the train instances
            double[] array = new double[number];
            for(int i=1;i<len;i++) {
                String[] t = tokens[i].split(":");
                int date = Integer.parseInt(t[0]);
                double num = Double.parseDouble(t[1]);
                while(date!=begin) {
                    array[begin-8] = 0.0;
                    begin++;
                }
                array[begin-8] = num;
                begin++;
            }
            while(begin<=22) {
                array[begin-8]= 0.0;
                begin++;
            }

            //method1: bp network
            double[][] traindata = new double[number-winsize][winsize+1];
            for(int i=winsize;i<number-1;i++) {
                for(int j=0;j<=winsize;j++) {
                    traindata[i-winsize][j] = array[i-winsize+j];
                }
            }
            //FastVector attrs = new FastVector();
            ArrayList<Attribute> attrs = new ArrayList<Attribute>();
            Attribute tag = new Attribute("tag",winsize);
            Attribute[] newattr = new Attribute[winsize];
            for(int i=0;i<winsize;i++) {
                newattr[i] = new Attribute("attr"+i,i);
                attrs.add(newattr[i]);
            }

            attrs.add(tag);
            Instances instancesTrain = new Instances("trainDataset",attrs,attrs.size());
            instancesTrain.setClass(tag);
            for(int i=0;i<number-winsize-1;i++) {
                Instance ins = new DenseInstance(attrs.size());
                ins.setDataset(instancesTrain);
                for(int j=0;j<=winsize;j++) {
                    ins.setValue(j,traindata[i][j]);
                }
                instancesTrain.add(ins);
            }


//            MultilayerPerceptron classifier = new MultilayerPerceptron();
            //LinearRegression classifier = new LinearRegression();
            SMOreg classifier = new SMOreg();

            try {
                classifier.buildClassifier(instancesTrain);
            } catch (Exception e) {
                e.printStackTrace();
            }

            for(int j=0;j<winsize;j++) {
                //ins.setValue(j,traindata[i][j]);
                traindata[number-winsize-1][j] = array[number-winsize-1+j];
            }

            // the test instances

            Instance target = new DenseInstance(attrs.size());
            target.setDataset(instancesTrain);
            for(int j=0;j<winsize;j++) {
                target.setValue(j,traindata[number-winsize-1][j]);

            }

            target.setValue(winsize,0);
            int predict=0;
            try {
                predict = new Double(classifier.classifyInstance(target)).intValue();
                if(predict<0) predict = 0;
                predict = (predict + (int)array[number-2])/2;
            } catch (Exception e) {
                e.printStackTrace();
            }


            //method2 :simple linearRegression
            /*
            double[][] traindata = new double[number-1][2];
            for(int i=0;i<number-1;i++) {
                traindata[i][0] = i+8;
                traindata[i][1] = array[i];
            }
            ArrayList<Attribute> attrs = new ArrayList<Attribute>();
            Attribute count = new Attribute("count",2);
            Attribute day = new Attribute("day",1);
            attrs.add(day);
            attrs.add(count);
            Instances instancesTrain = new Instances("trainDataset",attrs,attrs.size());
            instancesTrain.setClass(count);
            for(int i=0;i<number-1;i++) {
                Instance ins = new DenseInstance(attrs.size());
                ins.setDataset(instancesTrain);
                ins.setValue(day,traindata[i][0]);
                ins.setValue(count,traindata[i][1]);
                instancesTrain.add(ins);
            }
            LinearRegression linearRegres = new LinearRegression();
            try {
                linearRegres.buildClassifier(instancesTrain);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // the test instances
            Instance target = new DenseInstance(attrs.size());
            target.setDataset(instancesTrain);
            target.setValue(day,22);
            target.setValue(count,0);
            int predict=0;
            try {
                predict = new Double(linearRegres.classifyInstance(target)).intValue();
                if(predict<0) predict = 0;
            } catch (Exception e) {
                e.printStackTrace();
            }*/

            String[] windows= tokens[0].split(":");
            String window = windows[0]+"-"+windows[1].split("-")[1];
            //String real = tokens[tokens.length-1].split(":")[1];
            int real = (int)array[number-1];
            Text keyWord = new Text(window);
            Text valueWord = new Text(fileName + " " + predict + " " + real);
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
