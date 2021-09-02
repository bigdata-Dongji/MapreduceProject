package input_output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SlefFileInputformat {

public static class fileinput extends FileInputFormat<Text, BytesWritable>{

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        recordreader record = new recordreader();
        record.initialize(inputSplit,taskAttemptContext);
        return record;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
public static class recordreader extends RecordReader<Text,BytesWritable>{
    private FileSplit fileSplit;
    private Configuration configuration;
    private Text k=new Text();
    private BytesWritable value=new BytesWritable();
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit= (FileSplit) inputSplit;
        configuration=taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        Path path = fileSplit.getPath();
        byte[] by=new byte[(int) fileSplit.getLength()];
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream open = fileSystem.open(path);
        IOUtils.readFully(open,by,0,by.length);
        value.set(by,0,by.length);
        k.set(path.toString());
        IOUtils.closeStream(open);
        return false;

    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}

public static class SqenMap extends Mapper<Text,BytesWritable,Text,BytesWritable>{
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
public static class SqenReduce extends Reducer<Text,BytesWritable,Text,BytesWritable>{
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
//        context.getCounter().increment();
        context.write(key,values.iterator().next());
    }
}

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SlefFileInputformat.class);
        job.setMapperClass(SqenMap.class);
        job.setReducerClass(SqenReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(fileinput.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
