import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class BianMa {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Job job = Job.getInstance();

        job.setMapperClass(mymap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(myreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("G:\\QQ\\2020省赛\\QX\\meituan_waimai_meishi.csv"));
        FileOutputFormat.setOutputPath(job,new Path("G:\\QQ\\2020省赛\\QX\\out1"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class mymap extends Mapper<Object, Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String string = new String(value.getBytes(), "GBK");
//            String s = new String(value.getBytes(), "GBK");
            String string = value.toString();
            System.out.println(string);
            context.write(new Text(string),new Text(""));
        }
    }
    public static class myreduce extends Reducer<Text, Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }
}
