import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

// http://luchunli.blog.51cto.com/2368057/1719174
public class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>
        extends TextOutputFormat<K, V> {
    /**
     * OutputFormat通过获取Writer对象，将数据输出到指定目录特定名称的文件中。
     */
    private MultipleRecordWriter writer = null;

    // 在TextOutputFormat实现的时候对于每一个map或task任务都有一个唯一的标识，通过TaskID来控制，
    // 其在输出时文件名是固定的，每一个输出文件对应一个LineRecordWriter，取其输出流对象（FSDataOutputStream），
    // 在输出时通过输出流对象实现数据输出。
    //
    // 但是在这里实现的时候，实际上是要求对于一个task任务，将它需要输出的数据写入多个文件，文件是不固定的；
    // 因此在每次输出的时候判定对应的文件是否已经有Writer对象，若有则通过该对象继续输出，否则创建新的。
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (null == writer) {
            writer = new MultipleRecordWriter(context, this.getTaskOutputPath(context));
        }
        return writer;
    }

    // 获取任务的输出路径，仍然采用从committer中获取，TaskAttemptContext封装了task的上下文，后续分析。
    // 在TextOutputFormat中是通过调用父类（FileOutputFormat）的getDefaultWorkFile来实现的，
    // 而getDefaultWorkFile中获取MapReduce定义的默认的文件名，如需要自定义文件名，需自己实现
    private Path getTaskOutputPath(TaskAttemptContext context) throws IOException {
        Path workPath = null;
        OutputCommitter committer = super.getOutputCommitter(context);

        if (committer instanceof FileOutputCommitter) {
            // Get the directory that the task should write results into.
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            // Get the {@link Path} to the output directory for the map-reduce job.
            // context.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = super.getOutputPath(context);
            if (null == outputPath) {
                throw new IOException("Undefined job output-path.");
            }
            workPath = outputPath;
        }

        return workPath;
    }

    /**
     * @author luchunli
     * @description 自定义RecordWriter, MapReduce的TextOutputFormat的LineRecordWriter也是内部类，这里参照其实现方式
     */
    public class MultipleRecordWriter extends RecordWriter<K, V> {

        /** RecordWriter的缓存 **/
        private HashMap<String, RecordWriter<K, V>> recordWriters = null;

        private TaskAttemptContext context;

        /** 输出目录 **/
        private Path workPath = null;

        public MultipleRecordWriter () {}

        public MultipleRecordWriter(TaskAttemptContext context, Path path) {
            super();
            this.context = context;
            this.workPath = path;
            this.recordWriters = new HashMap<String, RecordWriter<K, V>>();
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            String baseName = generateFileNameForKeyValue (key, value, this.context.getConfiguration());
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);
            if (null == rw) {
                rw = this.getBaseRecordWriter(context, baseName);
                this.recordWriters.put(baseName, rw);
            }
            // 这里实际仍然为通过LineRecordWriter来实现的
            rw.write(key, value);
        }

        // 这里所做的工作就是屏蔽了到part-r-00000的输出，而是将同一个reduce的数据拆分为多个文件。
        private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext context, String baseName) throws IOException {
            Configuration conf = context.getConfiguration();

            boolean isCompressed = getCompressOutput(context);
            // 在LineRecordWriter的实现中，分隔符是通过变量如下方式指定的：
            // public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
            // String keyValueSeparator= conf.get(SEPERATOR, "\t");
            // 这里给了个逗号作为分割
            String keyValueSeparator = "";

            RecordWriter<K, V> rw = null;
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
                Path file = new Path(workPath, baseName + codec.getDefaultExtension());
                FSDataOutputStream out = file.getFileSystem(conf).create(file, false);
                rw = new LineRecordWriter<>(out, keyValueSeparator);
            } else {
                Path file = new Path(workPath, baseName);
                FSDataOutputStream out = file.getFileSystem(conf).create(file, false);
                rw = new LineRecordWriter<>(out, keyValueSeparator);
            }

            return rw;
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<K, V>> it = this.recordWriters.values().iterator();
            while (it.hasNext()) {
                RecordWriter<K, V> rw = it.next();
                rw.close(context);
            }
            this.recordWriters.clear();
        }

        /** 获取生成的文件的后缀名 **/
        private String generateFileNameForKeyValue(K key, V value, Configuration configuration) {
            char c = key.toString().toLowerCase().charAt(0);
            if (c >= 'a' && c <= 'z') {
                return c + ".txt";
            }
            return "other.txt";
        }
    }
}
