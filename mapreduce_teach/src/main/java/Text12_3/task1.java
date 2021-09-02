package Text12_3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class task1 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Job job=Job.getInstance();
        job.setJarByClass(task1.class);
        job.setMapperClass(mymap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(myreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("/test/Master_Loan_Summary.csv"));
        FileOutputFormat.setOutputPath(job,new Path("/test/out"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class  mymap extends Mapper<LongWritable, Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            if (split[0].contains("loan_number")){return;}
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date1 = new Date();
            Date date2 = new Date();
            Date date3 = new Date();
            try {
                date1=sdf.parse(split[6].replace("T00:00",""));
                date2=sdf.parse(split[13].replace("T00:00",""));
                date3=sdf.parse(split[14].replace("T00:00",""));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String origination_date = sdf.format(date1);
            String last_payment_date = sdf.format(date2);
            String next_payment_due_date = sdf.format(date3);
            String loan_status_description = "";
            if (split[16].contains("COMPLETED")){loan_status_description="已完成";}
            if (split[16].contains("CHARGEOFF")){loan_status_description="冲销";}
            if (split[16].contains("CURRENT")){loan_status_description="进行中";}
            if (split[16].contains("DEFAULTED")){loan_status_description="违约";}
            String grade = "";
            if (Double.parseDouble(split[3])>0 &&Double.parseDouble(split[3])<=0.05){grade="A";}
            if (Double.parseDouble(split[3])>0.05 &&Double.parseDouble(split[3])<=0.1){grade="B";}
            if (Double.parseDouble(split[3])>0.1 &&Double.parseDouble(split[3])<=0.15){grade="C";}
            if (Double.parseDouble(split[3])>0.15 &&Double.parseDouble(split[3])<=0.2){grade="D";}
            if (Double.parseDouble(split[3])>0.2 &&Double.parseDouble(split[3])<=0.25){grade="E";}
            if (Double.parseDouble(split[3])>0.25 &&Double.parseDouble(split[3])<=0.3){grade="F";}
            if (Double.parseDouble(split[3])>0.3){grade="G";}
            context.write(new Text(grade+","+origination_date),new Text(split[0]+","+split[1]+","+split[2]+","+split[3]+","+split[4]+","+grade+","+split[7]+","+split[8]+","+split[9]+","+split[10]+","+split[11]+","+split[12]+","+last_payment_date+","+next_payment_due_date+","+
                    split[15]+","+loan_status_description+","+split[17]));

        }
    }
    public static class  myreduce extends Reducer<Text, Text,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (Text value : values){
                if (count<3){
                    context.write(new Text(key.toString()+","+value),NullWritable.get());
                }
                count++;
            }
        }
    }
}
