package case_2;

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

public class task3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setJarByClass(task3.class);
        job.setMapperClass(task3_Map.class);
        job.setPartitionerClass(Par.class);
        job.setReducerClass(task3_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path in=new Path("E:\\数据文件\\out\\case_2_task1_out\\part-r-00000");
        Path out=new Path("E:\\数据文件\\out\\case_2_task3_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class task3_Map extends Mapper<Object, Text,Text,NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines=value.toString().split(",");
            StringBuilder stringBuilder=new StringBuilder();
            stringBuilder.append(lines[0]);stringBuilder.append("\t");
            stringBuilder.append(lines[2]);stringBuilder.append("\t");
            stringBuilder.append(lines[1]);stringBuilder.append("\t");
            stringBuilder.append(lines[3]);stringBuilder.append("\t");
            stringBuilder.append(lines[5]);
            StringBuilder stringBuilder1=new StringBuilder();
            stringBuilder1.append(lines[0]);stringBuilder1.append("\t");
            stringBuilder1.append(lines[4]);
            context.write(new Text("l+"+stringBuilder.toString()), NullWritable.get());
            context.write(new Text("r+"+stringBuilder1.toString()),NullWritable.get());
        }
    }
    public static class Par extends Partitioner<Text,NullWritable>{
        @Override
        public int getPartition(Text text, NullWritable nullWritable, int i) {
            if (text.toString().startsWith("l+")){return 0;}
            else return 1;
        }
    }
    public static class task3_Reduce extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString().substring(2)),NullWritable.get());
        }
    }
}
