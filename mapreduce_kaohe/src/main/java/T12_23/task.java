package T12_23;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class task {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Job job = Job.getInstance();

        job.setJarByClass(task.class);
        job.setMapperClass(mymap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(myreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("/datas/1215.json"));
        FileOutputFormat.setOutputPath(job,new Path("/datas/out"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class mymap extends Mapper<Object, Text,Text, NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = JSON.parseObject(value.toString());
            //提取数据
            String shop_id = jsonObject.getString("shop_id");
            String shop_url = jsonObject.getString("shop_url");
            String title = jsonObject.getString("title");
            String popularity = jsonObject.getString("popularity");
            String type = jsonObject.getString("type");
            String price = jsonObject.getString("price");
            String autoCount = jsonObject.getString("autoCount");
            String crazy_count = jsonObject.getString("crazy_count");
            String goodRate = jsonObject.getString("goodRate");
            String indifferent_count = jsonObject.getString("indifferent_count");
            String image_count = jsonObject.getString("image_count");
            //去除含空字段数据
            if (shop_id==null||shop_url==null||title==null||popularity==null||type==null||price==null||autoCount==null||crazy_count==null||goodRate==null||indifferent_count==null||image_count==null){
                return;
            }
            //封装数据，以逗号分隔
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(shop_id).append(",");
            stringBuffer.append(shop_url).append(",");
            stringBuffer.append(title).append(",");
            stringBuffer.append(popularity).append(",");
            stringBuffer.append(type).append(",");
            stringBuffer.append(price).append(",");
            stringBuffer.append(autoCount).append(",");
            stringBuffer.append(crazy_count).append(",");
            stringBuffer.append(goodRate).append(",");
            stringBuffer.append(indifferent_count).append(",");
            stringBuffer.append(image_count);
            context.write(new Text(stringBuffer.toString()),NullWritable.get());
        }
    }
    public static class myreduce extends Reducer<Text, NullWritable,Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}
