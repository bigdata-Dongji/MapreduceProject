package text;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * UserLoseDriver
 * 根据用户行为数据，编写 MapReduce 程序来统计出商品点击量排行。
 */
public class ItemClickRankDriver {

    public static class ThisMap extends Mapper<Object, Text, Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            /********** Begin **********/
//            Calendar calendar =Calendar.getInstance();

            //1. 分割每行数据
            String[] lines=value.toString().split(",");

            //2. 得到商品id
            String name=lines[1];

            //3. 得到行为属性
            String shu=lines[3];

            //4. 如果行为属性是 'pv'，则写入到map输出
            if(shu.contains("pv")){
                context.write(new Text(name),one);
            }

            /********** End **********/
        }
    }

    public static class ThisReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        //对象实例，用来保存reduce方法中处理的数据
        //List<Integer> list=new ArrayList<Integer>();
        Map<String, Integer> map = new TreeMap<String, Integer>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            /********** Begin **********/
            int sum = 0;
            // 统计同key总数， 把key和sum写入到list中
            List<String> list = new ArrayList<>();
            for(IntWritable i:values){
                sum += i.get();
            }
            map.put(key.toString(),sum);
            /********** End **********/
        }

        //cleanup方法，即reduce对象执行完所有的reduce方法后最后执行的方法
        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            /********* 可能使用 ***********/
            //这里将map.entrySet()转换成list
            List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>(map.entrySet());

            //然后通过比较器来实现排序
            Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {
                //升序排序
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            for(Map.Entry<String,Integer> mapping:list)
                context.write(new Text(mapping.getKey()), new IntWritable(mapping.getValue()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "商品点击量排行");

        job.setJarByClass(ItemClickRankDriver.class);
        job.setMapperClass(ThisMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ThisReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
