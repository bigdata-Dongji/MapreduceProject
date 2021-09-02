package text;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class ItemClickTopOneEachTypeDriver {

    public static class ThisMap extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            /********** Begin **********/
            String[] lines=value.toString().split(",");
            context.write(new Text(lines[2]),new Text(lines[1]));
            /********** End **********/

        }
    }

    public static class ThisReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 提示: 先得出所有商品id的数量，再从这些数量中找出最大值
            /*** 在这编写reduce内容 ****/
            /********** Begin **********/

            HashMap<String, Integer> map = new HashMap<>();
            List<String> list = new ArrayList<>();
            for(Text i:values){
                list.add(i.toString());
            }

            for(String j:list){
                Integer count = map.get(j);
                map.put(j, (count == null) ? 1 : count + 1);
            }

            Collection<Integer> c = map.values();
            Object[] obj = c.toArray();
            Arrays.sort(obj);
            String value=obj[obj.length-1].toString();

            //根据value取key
            String keys="";
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                String objs = entry.getValue().toString();
                if (objs != null && objs.equals(value)) {
                    keys=  entry.getKey().toString();
                }
            }
            for (String mapset:map.keySet()){
                mapset.toString();
                map.get(mapset);
            }


//            Map<String, Integer> resultMap = sortMapByValue(map);
//            for(Map.Entry<String,Integer> mapping:resultMap.entrySet()){
//                context.write(new Text(mapping.getKey()), new IntWritable(mapping.getValue()));
//            }
//            for (Map.Entry<String, Integer> m :map.entrySet())  {
//                System.out.println(m.getKey()+"\t"+m.getValue());
//                double a=(double)89/m.getValue();
//                keys=m.getKey();
//                DoubleWritable doubleWritable = new DoubleWritable(a);
//            }
            {
                context.write(new Text(key.toString()), new Text(keys));
            }
//            Set<String> set = map.keySet();
//            Object[] obj = set.toArray();
//            Arrays.sort(obj);
//            Collection<Integer> c = map.values();
//            Object[] objs = c.toArray();
//            Arrays.sort(objs);
//            int a=obj.length;
//            System.out.print(obj[obj.length-1].toString());
//            context.write(new Text(obj[obj.length-1].toString()),new Text(objs[objs.length-1].toString()));
            // context.write(new Text((String)obj[length - 1]),new Text(""));

            /********** End **********/
        }

    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "各个商品类别中点击量最高的商品");

        job.setJarByClass(ItemClickTopOneEachTypeDriver.class);
        job.setMapperClass(ThisMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ThisReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("F:\\Educoder大数据\\中级\\电商数据\\text.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\text_out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
