package case_2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class task1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setJarByClass(task1.class);
        job.setMapperClass(task1_Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        Path in=new Path("E:\\数据文件\\1215.json");
        Path out=new Path("E:\\数据文件\\out\\case_2_task1_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class task1_Map extends Mapper<Object, Text,Text,NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject= JSON.parseObject(value.toString());
            String shop_id = jsonObject.getString("shop_id");
            String shop_url = jsonObject.getString("shop_url");
            String title = jsonObject.getString("title");
            String popularity = jsonObject.getString("popularity");
            String type = jsonObject.getString("type");
            String price= jsonObject.getString("price");
            String autoCount = jsonObject.getString("autoCount");
            String crazy_count = jsonObject.getString("crazy_count");
            String goodRate = jsonObject.getString("goodRate");
            String iindifferent_count = jsonObject.getString("iindifferent_count");
            String image_count = jsonObject.getString("image_count");
            if (shop_id==null||title==null||popularity==null||type==null||price==null||crazy_count==null)return;
            StringBuilder stringBuilder=new StringBuilder();
            stringBuilder.append(shop_id);stringBuilder.append(",");
            stringBuilder.append(shop_url);stringBuilder.append(",");
            stringBuilder.append(title);stringBuilder.append(",");
            stringBuilder.append(popularity);stringBuilder.append(",");
            stringBuilder.append(type);stringBuilder.append(",");
            stringBuilder.append(price);stringBuilder.append(",");
            stringBuilder.append(autoCount);stringBuilder.append(",");
            stringBuilder.append(crazy_count);stringBuilder.append(",");
            stringBuilder.append(goodRate);stringBuilder.append(",");
            stringBuilder.append(iindifferent_count);stringBuilder.append(",");
            stringBuilder.append(image_count);
            context.write(new Text(stringBuilder.toString()),NullWritable.get());
        }
    }
}
