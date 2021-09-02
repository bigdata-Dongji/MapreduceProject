import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class task2 {
    public static class taskMap extends Mapper<LongWritable, Text,Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //切割数据
            String[] lines=value.toString().split(",");
//            System.out.println(lines[8]);
            //去除上映天数是零点场，点映，展映，重映的数据
            if(lines[8].contains("零点场")){return;}
            if(lines[8].contains("点映")){return;}
            if(lines[8].contains("展映")){return;}
            if(lines[8].contains("重映")){return;}
            //定义上映日期
            String Release_date;
            //定义日期格式化
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");
            //“上映天数”为空，则上映日期为‘往期电影’
            if(lines[8].isEmpty()){
                Release_date = "往期电影";
            }
            else if(lines[8].contains("上映首日")){//“上映天数”为上映首日，则上映日期为当前日期
                Release_date=lines[9];
            }
            else{
                Date date;
                try {
                    //将当前日期格式化
                    date = simpleDateFormat.parse(lines[9]);
                } catch (ParseException e) {
                    return;
                }
                //定义日期运算的对象
                Calendar calendar = Calendar.getInstance();
                //设置日期
                calendar.setTime(date);
                //取上映天数
                String day=lines[8].replace("上映","").replace("天","");
//                System.out.println(day);
                //上映日期=当前日期-上映天数
                calendar.set(Calendar.DATE, calendar.get(Calendar.DATE)-Integer.parseInt(day));
                //格式化上映日期
                Release_date=simpleDateFormat.format(calendar.getTime());
            }
//            System.out.println(Release_date);
            BigDecimal boxoffice = null;
            BigDecimal total_boxoffice = null;
            //当日综合票房：换算亿转万
            if(lines[1].contains("亿")){
                String now_day=lines[1].replace("亿","");
                double one=Double.parseDouble(now_day)*10000.0;
                boxoffice=new BigDecimal(Double.toString(one));
            }
            //当日综合票房：万，去掉万
            else if(lines[1].contains("万")){
                String W_day=lines[1].replace("万","");
                boxoffice=new BigDecimal(W_day);
            }
            //当前总票房：换算亿转万
            if(lines[7].contains("亿")){
                String total=lines[7].replace("亿","");
                double two=Double.parseDouble(total)*10000.0;
                total_boxoffice = new BigDecimal(Double.toString(two));
            }
            //当前总票房：万，去掉万
            else if(lines[7].contains("万")){
                String W_total=lines[7].replace("万","");
                total_boxoffice=new BigDecimal(W_total);
            }
            context.write(new Text(lines[0] + "\t" + boxoffice + "\t" + lines[2] + "\t" + lines[3] + "\t" + lines[4] + "\t" + lines[5] + "\t" + lines[6] + "\t" + total_boxoffice + "\t" + lines[8] + "\t" + lines[9] + "\t" + Release_date),NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        BasicConfigurator.configure();
        Job job=Job.getInstance();
        job.setJarByClass(task2.class);
        job.setMapperClass(taskMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

