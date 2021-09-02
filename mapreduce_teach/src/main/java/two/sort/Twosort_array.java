package two.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;
import java.util.*;

public class Twosort_array {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Twosort_array.class);

        job.setMapperClass(mymap.class);
        job.setReducerClass(myreduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("E:\\数据文件\\goods_visit2"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\text"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class mymap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(" ");
            context.write(new IntWritable(Integer.parseInt(split[1])),new IntWritable(Integer.parseInt(split[0])));
        }
    }
    public static class myreduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> list=new ArrayList<Integer>();
            for (IntWritable j:values){
                list.add(j.get());
            }


            Collections.sort(list);
//            System.out.println(list);
//            System.out.println("------------------");
//            Collections.reverse(list);
//            System.out.println(list);
            for (Integer i:list){
                context.write(key,new IntWritable(i));
            }
        }
    }
}
