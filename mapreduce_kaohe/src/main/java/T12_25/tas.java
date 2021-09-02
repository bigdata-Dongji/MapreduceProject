package T12_25;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;

public class tas {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Job job = Job.getInstance();
        job.setJarByClass(tas.class);
        job.setMapperClass(mymap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(myreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/datas/coronavirus_outbreak.json"));
        FileOutputFormat.setOutputPath(job,new Path("/datas/out1"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class mymap extends Mapper<Object, Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //修改编码
            String sss=new String(value.getBytes(),"GBK");
            //去除前后【】
            if (sss.contains("[")||sss.contains("]")){return;}
            //数据问题
            String substring = sss.substring(sss.indexOf("{"), sss.indexOf("}")+1);
            //提取json数据
            JSONObject jsonObject = JSON.parseObject(substring);
            String date = jsonObject.getString("date");
            String city = jsonObject.getString("city");
            String newly_diagnosed = jsonObject.getString("newly_diagnosed");
            String add_cure = jsonObject.getString("add_cure");
            String new_death = jsonObject.getString("new_death");
            //去除后三个字段都为空的
            if (newly_diagnosed.isEmpty()&&add_cure.isEmpty()&&new_death.isEmpty()){return;}
            //空的补”0“
            if (newly_diagnosed.isEmpty()){newly_diagnosed="0";}if (add_cure.isEmpty()){add_cure="0";}
            if (new_death.isEmpty()){new_death="0";}
            //去除重复的日期城市
            context.write(new Text(date+","+city),new Text(newly_diagnosed+","+add_cure+","+new_death));
        }
    }
    public static class myreduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:values){
                context.write(key,value);
                break;
            }
        }
    }
}
