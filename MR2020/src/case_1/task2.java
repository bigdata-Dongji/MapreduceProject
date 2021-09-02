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
import java.util.Calendar;


public class task2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setMapperClass(task2_Map.class);
        job.setReducerClass(task2_Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path in=new Path("E:\\数据文件\\附件1.txt");
        Path out=new Path("E:\\数据文件\\out\\case_1_task2_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class task2_Map extends Mapper<Object, Text,Text, DoubleWritable>{
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
            if (!month.equals("04"))return;
            context.write(new Text(lines[6]),new DoubleWritable(Double.parseDouble(lines[3])));
        }
    }

    public static class task2_Reduce extends Reducer<Text,DoubleWritable,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            double price=0.0;
            for (DoubleWritable i:values){
                price+=i.get();
                sum++;
            }
            context.write(new Text(key),new Text("四月份交易额："+price+"\t"+"四月份订单量："+sum));
        }
    }
}
