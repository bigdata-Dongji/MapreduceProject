package case_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class task2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(task2.class);
        ChainMapper.addMapper(job,task2_Map1.class,Object.class,Text.class,Text.class,NullWritable.class,conf);
        ChainMapper.addMapper(job,task2_Map2.class,Text.class,NullWritable.class,Text.class,NullWritable.class,conf);
        ChainReducer.setReducer(job,task2_Reduce.class,Text.class,NullWritable.class,Text.class,NullWritable.class,conf);
        Path in=new Path("E:\\数据文件\\out\\case_2_task1_out\\part-r-00000");
        Path out=new Path("E:\\数据文件\\out\\case_2_task2_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class task2_Map1 extends Mapper<Object, Text,Text,NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines=value.toString().split(",");
            if (Double.parseDouble(lines[3])<1000)return;
            context.write(value, NullWritable.get());
        }
    }
    public static class task2_Map2 extends Mapper<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            String[] lines=key.toString().split(",");
            if(Double.parseDouble(lines[5])<100||Double.parseDouble(lines[5])>200)return;
            context.write(key,NullWritable.get());
        }
    }
    public static class task2_Reduce extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] lines = key.toString().split(",");
            if (Double.parseDouble(lines[8]) < 90) return;
            context.write(key, NullWritable.get());
        }
    }
}
