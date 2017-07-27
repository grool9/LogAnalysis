import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.gui.beans.AbstractTrainingSetProducerBeanInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 7/26/17.
 */
public class PredictGlobal {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = (fileSplit.getPath().getName().split("\\.txt"))[0];
            context.write(new Text(fileName),value);

        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        static ArrayList<String> dataset = new ArrayList<String>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                dataset.add(key.toString()+" "+value.toString());
                //context.write(key, value);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            int number = 15;
            int begin ;
            int winsize = 13;
            //int[] realvalues ;
            ArrayList<Integer> realvalues = new ArrayList();
            ArrayList<Integer> corrected = new ArrayList<Integer>();
            ArrayList<Attribute> attrs = new ArrayList<Attribute>();
            Attribute tag = new Attribute("tag",winsize+2);
            List url_values = new ArrayList();
            url_values.add("null");
            url_values.add("tour-category-ids-query");
            url_values.add("tour-category-query");
            url_values.add("tour-category-scenic-query");
            url_values.add("tour-category-statis-query");
            url_values.add("tour-category-tuniu-hot-query");
            url_values.add("tour-category-vendor-statis-query");
            url_values.add("tour-category-weekendproduct-query");
            url_values.add("tour-em-task-exec");
            url_values.add("tour-faq-query");
            url_values.add("tour-flight-ticket-query");
            url_values.add("tour-guide-delete");
            url_values.add("tour-guide-query");
            url_values.add("tour-guide-update");
            url_values.add("tour-hotel-query");
            url_values.add("tour-hotel-search-nearby-scenic-query");
            url_values.add("tour-hotel-search-query");
            url_values.add("tour-hotelSuggestion-query");
            url_values.add("tour-phoenix-product-query");
            //url_values.add("tour-poi-query-queryCategory-");
            url_values.add("tour-poi-query-queryCategory");
            url_values.add("tour-poi-query-queryNumberFound");
            url_values.add("tour-poi-query-queryProvinceList");
            url_values.add("tour-poi-query-queryScenicNumPerCity");
            url_values.add("tour-poi-query-queryScenicSpotCount");
            url_values.add("tour-poi-query-queryScenicTypeList");
            url_values.add("tour-poi-query");
            url_values.add("tour-poi-scenictype-provincelist-query");
            url_values.add("tour-product-query");
            url_values.add("tour-suggestion-query");
            Attribute urltag = new Attribute("urltag",url_values,winsize);

            List time_values = new ArrayList();
            time_values.add("0:00-1:00");
            time_values.add("1:00-2:00");
            time_values.add("2:00-3:00");
            time_values.add("3:00-4:00");
            time_values.add("4:00-5:00");
            time_values.add("5:00-6:00");
            time_values.add("6:00-7:00");
            time_values.add("7:00-8:00");
            time_values.add("8:00-9:00");
            time_values.add("9:00-10:00");
            time_values.add("10:00-11:00");
            time_values.add("11:00-12:00");
            time_values.add("12:00-13:00");
            time_values.add("13:00-14:00");
            time_values.add("14:00-15:00");
            time_values.add("15:00-16:00");
            time_values.add("16:00-17:00");
            time_values.add("17:00-18:00");
            time_values.add("18:00-19:00");
            time_values.add("19:00-20:00");
            time_values.add("20:00-21:00");
            time_values.add("21:00-22:00");
            time_values.add("22:00-23:00");
            time_values.add("23:00-0:00");

            Attribute timetag = new Attribute("timetag",time_values,winsize+1);
            Attribute[] newattr = new Attribute[winsize];
            for(int i=0;i<winsize;i++) {
                newattr[i] = new Attribute("attr"+i,i);
                attrs.add(newattr[i]);
            }
            attrs.add(urltag);
            attrs.add(timetag);
            attrs.add(tag);
            Instances instancesTrain = new Instances("trainDataset",attrs,attrs.size());
            Instances instancesTest = new Instances("testDataset", attrs, attrs.size());
            instancesTrain.setClass(tag);
            instancesTest.setClass(tag);
            for(String line:dataset) {
                String[] tokens = line.split("\\s+");
                int len = tokens.length;
                begin = 8;
                double[] array = new double[number];
                for(int i=2;i<len;i++) {
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
                double[][] traindata = new double[number-winsize-1][winsize+1];
                for(int i=winsize;i<number-1;i++) {
                    for(int j=0;j<=winsize;j++) {
                        traindata[i-winsize][j] = array[i-winsize+j];
                    }
                }
                for(int i=0;i<number-winsize-1;i++) {
                    Instance ins = new DenseInstance(attrs.size());
                    ins.setDataset(instancesTrain);
                    for(int j=0;j<winsize;j++) {
                        ins.setValue(j,traindata[i][j]);
                    }
                    ins.setValue(urltag,tokens[0]);
                    ins.setValue(timetag,tokens[1]);
                    ins.setValue(tag,traindata[i][winsize]);
                    instancesTrain.add(ins);
                }
                realvalues.add((int)array[number-1]);
                corrected.add((int)array[number-2]);
                Instance target = new DenseInstance(attrs.size());
                target.setDataset(instancesTrain);
                for(int j=0;j<winsize;j++) {
                    target.setValue(j,array[number-winsize-1+j]);
                }
                target.setValue(tag,10);
                target.setValue(urltag,tokens[0]);
                target.setValue(timetag,tokens[1]);
                instancesTest.add(target);
            }
            MultilayerPerceptron classifier = new MultilayerPerceptron();
            //LinearRegression classifier = new LinearRegression();
            try {
                classifier.buildClassifier(instancesTrain);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // the test instances
            for(int i=0;i<instancesTest.size();i++) {
                Instance target = instancesTest.get(i);
                int predict=10;
                try {
                    predict = new Double(classifier.classifyInstance(target)).intValue();
                    if(predict<0) predict = corrected.get(i);
                    else predict = (predict + corrected.get(i))/2;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String[] windows= target.stringValue(timetag).split(":");
                String window = windows[0]+"-"+windows[1].split("-")[1];
                //String real = tokens[tokens.length-1].split(":")[1];
                int real = realvalues.get(i);
                Text keyWord = new Text(window);
                Text valueWord = new Text(target.stringValue(urltag) + " " + predict + " " + real);
                context.write(keyWord, valueWord);
            }

        }
    }
}
