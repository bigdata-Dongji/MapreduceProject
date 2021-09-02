import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Wordcount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job=Job.getInstance();
        job.setJobName("WordCount");
        job.setJarByClass(Wordcount.class);
        job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);//运行效率更好,运行两次reduce
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path in=new Path("E:\\数据文件\\buyer_favorite1");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,new Path("E:\\数据文件\\out\\WordCountOut"));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class Map extends Mapper<Object, Text,Text, IntWritable> {
        private static final Text newkey=new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keys=value.toString().split("\t");
            newkey.set(keys[0]);
            context.write(newkey,new IntWritable(1));
        }

    }
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable value:values){
                sum+=1;
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
