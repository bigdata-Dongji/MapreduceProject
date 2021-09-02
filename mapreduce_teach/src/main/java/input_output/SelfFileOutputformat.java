package input_output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SelfFileOutputformat{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SlefFileInputformat.class);
        job.setMapperClass(selfmap.class);
        job.setReducerClass(selfreducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        job.setOutputFormatClass(Selfoutput.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //虽然定义了outputformat，但因为outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
    public static class Selfoutput extends FileOutputFormat<Text, NullWritable>{

        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new SelfRecordWriter(taskAttemptContext);
        }
    }

    public static class SelfRecordWriter extends RecordWriter<Text, NullWritable>{
        FSDataOutputStream leftDataOutputStream;
        FSDataOutputStream rightDataOutputStream;

        public SelfRecordWriter(TaskAttemptContext taskAttemptContext) {
            try {
                FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());

                leftDataOutputStream = fileSystem.create(new Path("E:\\数据文件\\out\\left"));
                rightDataOutputStream = fileSystem.create(new Path("E:\\数据文件\\out\\right"));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

            if (text.toString().contains("left")){
                leftDataOutputStream.write(text.toString().getBytes());
            }else rightDataOutputStream.write(text.toString().getBytes());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            IOUtils.closeStream(leftDataOutputStream);
            IOUtils.closeStream(rightDataOutputStream);
        }
    }



    public static class selfmap extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }
    public static class selfreducer extends Reducer<Text,NullWritable,Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}

