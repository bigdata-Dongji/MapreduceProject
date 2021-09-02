package case_1;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class task1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setMapperClass(Map.class);
        job.setPartitionerClass(Par.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(5);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path in=new Path("E:\\数据文件\\附件1.txt");
        Path out=new Path("E:\\数据文件\\out\\case_1_task1_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class Map extends Mapper<Object, Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines= value.toString().split(",");
            context.write(new Text(lines[6]),new Text(value));
        }
    }

    public static class Par extends Partitioner<Text,Text>{
//        HashMap
        @Override
        public int getPartition(Text text, Text text2, int i) {
            if (text.toString().equals("A"))return 0;
            else if (text.toString().equals("B"))return 1;
            else if (text.toString().equals("C"))return 2;
            else if (text.toString().equals("D"))return 3;
            else return 4;
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text i : values){
                context.write(i,NullWritable.get());
            }
        }
    }
}
