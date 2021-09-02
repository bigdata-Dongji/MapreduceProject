
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;


import java.util.Arrays;
import java.util.Date;
import java.util.StringTokenizer;

public class CleanData {

    public static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer lines = new StringTokenizer(value.toString(),"\n");
            //定义清洗结果
            while (lines.hasMoreTokens()){
                String[] arr = lines.nextToken().split("\\s");
                // 将域名切分
                String domain = arr[4];
                arr[4] = domain.split("/")[0];
                // 时间转换
                String timeStramp = arr[arr.length - 1];
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                arr[arr.length - 1] = format.format(new Date(Long.parseLong(timeStramp)));
                context.write(new Text(StringUtils.join(" ", arr)),NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CleanData.class);
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        String inputfile = "E:\\数据文件\\unclear\\*";
        String outputFile = "E:\\数据文件\\out\\unclear_out";
        FileInputFormat.addInputPath(job, new Path(inputfile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
