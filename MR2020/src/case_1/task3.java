package case_1;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class task3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setJarByClass(task3.class);
        job.setMapperClass(task3_Map.class);
        job.setReducerClass(task3_Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path in=new Path("E:\\数据文件\\附件1.txt");
        Path out=new Path("E:\\数据文件\\out\\case_1_task3_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class task3_Map extends Mapper<Object, Text,Text, DoubleWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines=value.toString().split(",");
            if (lines[0].equals("订单号")){return;}
            String month= null;
            try {
                month = new SimpleDateFormat("MM").format(new SimpleDateFormat("yyyy/MM/dd").parse(lines[5]));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            context.write(new Text(lines[6]+month),new DoubleWritable(Double.parseDouble(lines[3])));
        }
    }
    public static class task3_Reduce extends Reducer<Text,DoubleWritable,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int month[]={31,28,31,30,31,30,31,31,30,31,30,31};
            int sum=0;
            double price=0;
            for (DoubleWritable i:values){
                price+=i.get();
                sum++;
            }
            context.write(key,new Text("月平均交易额："+price/sum+"\t"+"日均订单量："+sum/month[Integer.parseInt(key.toString().substring(1))-1]));
        }
    }
}
