package T12_25;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class fenxi {
    public static void main(String[] args) throws InterruptedException, Exception, ClassNotFoundException {
        BasicConfigurator.configure();
        Job job = Job.getInstance();
        job.setJarByClass(tas.class);
        job.setMapperClass(mymap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(myreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("G:\\QQ\\2020省赛\\综合测评6-1225\\test_data\\world_total_data.csv"));
        FileOutputFormat.setOutputPath(job,new Path("G:\\QQ\\2020省赛\\综合测评6-1225\\test_data\\out1"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class mymap extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String string=new String(value.getBytes(),"GBK");
            if (string.contains("计数")){return;}
            String[] split = string.substring(0,string.length()-1).split(",");
            String date = split[9].substring(0, 6);
            double s=Integer.parseInt(split[4])/Integer.parseInt(split[2]);
            context.write(new Text(date),new Text(s+","+split[2]));
        }
    }
    public static class myreduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum= 0;
            int sumque= 0;
            for (Text value:values){
                String[] split = value.toString().split(",");
                sum+=Double.parseDouble(split[0]);
                sumque+=Integer.parseInt(split[1]);
            }
            context.write(key,new Text("治愈率:"+sum+",确诊病例:"+sumque));
        }
    }
}
