import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Movie {
    public static class MovieMap extends Mapper<LongWritable, Text,Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines=value.toString().split(",");
            if(lines[8].contains("零点场")){return;}
            if(lines[8].contains("点映")){return;}
            if(lines[8].contains("展映")){return;}
            if(lines[8].contains("重映")){return;}
            String Release_date;

//            System.out.println(lines[9].getClass().getTypeName());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            if(lines[8].isEmpty()){
                Release_date = "往期电影";
            }
            else if(lines[8].contains("上映首日")){
                Release_date=lines[9];
            }
            else{
                Date date;
                try {
                    date = sdf.parse(lines[9]);
                } catch (ParseException e) {
                    return;
                }
                Calendar cld = Calendar.getInstance();
                cld.setTime(date);
                String day=lines[8].substring(lines[8].indexOf("映")+1,lines[8].indexOf("天"));
//                cld.set(Calendar.DATE,cld.get(Calendar.DATE)-1);
                cld.set(Calendar.DATE, cld.get(Calendar.DATE)-Integer.parseInt(day));
                Release_date=sdf.format(cld.getTime());
            }
            BigDecimal piao_one = null;
            BigDecimal piao_two = null;
            if(lines[1].contains("亿")){
                String num_one=lines[1].substring(0,lines[1].indexOf("亿"));
                double one=Double.parseDouble(num_one)*10000.0;
                piao_one=new BigDecimal(Double.toString(one));
            }
            else if(lines[1].contains("万")){
                String num_one_day=lines[1].substring(0,lines[1].indexOf("万"));
                piao_one=new BigDecimal(num_one_day);
            }
            if(lines[7].contains("亿")){
                String num_two=lines[7].substring(0,lines[7].indexOf("亿"));
                double two=Double.parseDouble(num_two)*10000.0;
                piao_two = new BigDecimal(Double.toString(two));
            }
            else if(lines[7].contains("万")){
                String num_two_day=lines[7].substring(0,lines[7].indexOf("万"));
                piao_two=new BigDecimal(num_two_day);
            }
//            System.out.println(lines[0]+'\t'+piao_one+'\t'+piao_two+'\t'+Release_date);
            context.write(new Text(lines[0] + "\t" + piao_one + "\t" + lines[2] + "\t" + lines[3] + "\t" + lines[4] + "\t" + lines[5] + "\t" + lines[6] + "\t" + piao_two + "\t" + lines[8] + "\t" + lines[9] + "\t" + Release_date),NullWritable.get());
        }
    }
    public static class MovieReduce extends Reducer<Text,NullWritable,NullWritable,Text> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),new Text(key));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        BasicConfigurator.configure();
        FileSystem fileSystem=FileSystem.get(new URI("file:///E:/数据文件/out/movie_out"),new Configuration());
        if (fileSystem.exists(new Path("E:\\数据文件\\out\\movie_out"))){
            fileSystem.delete(new Path("E:\\数据文件\\out\\movie_out"),true);
        }
        Job job=Job.getInstance();
        job.setJarByClass(Movie.class);
        job.setMapperClass(MovieMap.class);
        job.setReducerClass(MovieReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("E:\\数据文件\\movies.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\数据文件\\out\\movie_out"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
