package case_2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class task6 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setMapperClass(task6_Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        Path in=new Path("E:\\数据文件\\out\\case_2_task2_out\\part-r-00000");
        Path out=new Path("E:\\数据文件\\out\\case_2_task6_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }


    public static class task6_Map extends Mapper<Object,Text,Text,NullWritable> {
        HashMap<String,String> map1=new HashMap<String,String>();
        HashMap<String,String> map2=new HashMap<String,String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader b1=new BufferedReader(new FileReader("E:\\数据文件\\out\\case_2_task4_out\\part-r-00000"));
            String line1=null;
            while ((line1=b1.readLine())!=null){
                String[] split = line1.split("\t");
                map1.put(split[0],split[1]+'\t'+split[2]+'\t'+split[3]);
            }
            b1.close();
            BufferedReader b2=new BufferedReader(new FileReader("E:\\数据文件\\out\\case_2_task5_out\\part-r-00000"));
            String line2=null;
            while ((line2=b2.readLine())!=null){
                String[] split = line2.split("\t");
                map2.put(split[0],split[1]+'\t'+split[2]);
            }
            b2.close();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            if (map1.containsKey(split[0])&&map2.containsKey(split[0])){
                context.write(value, NullWritable.get());
            }
        }
    }
}
