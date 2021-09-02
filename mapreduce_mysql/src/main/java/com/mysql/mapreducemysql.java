package com.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class mapreducemysql {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration conf=new Configuration();
        //输入文件KeyValue类型
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/crawler",
                "root",
                "511722");
        Job job=Job.getInstance(conf);
        job.setJarByClass(mapreducemysql.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置输入文件读取类型
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //设置切片大小为4M，优化MapReduce处理小文件效率
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        FileInputFormat.addInputPath(job,new Path("G:\\360MoveData\\Users\\dongmiaomiao\\Desktop\\text.txt"));

        DBOutputFormat.setOutput(job,"text", new String[]{"keyword", "total"});
        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class Map extends Mapper<Object, Text,Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines=value.toString().split(" ");
            for (String i:lines){
                if (i.isEmpty()){return;}
            }
            context.write(new Text(value.toString()),new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,DBinput,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable i:values){
                sum=sum+Integer.parseInt(i.toString());
            }
            DBinput dBinput=new DBinput(key.toString(),sum);
            context.write(dBinput,NullWritable.get());
        }
    }

}
