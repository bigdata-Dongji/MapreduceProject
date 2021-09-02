package MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import sun.nio.cs.ext.MacCentralEurope;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class MapPhone {
    public static class mymap extends Mapper<Object, Text,Text,NullWritable> {
        HashMap<String, String> hsMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //读取缓存文件
            URI[] cacheFiles = context.getCacheFiles();
            String path = cacheFiles[0].getPath();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            //判断每一行是否有数据
            String line;
            while (StringUtils.isNotEmpty(line=reader.readLine())){
                String[] split = line.split("\t");
                hsMap.put(split[0],split[1]);
            }
            //关闭资源
            IOUtils.closeStream(reader);
        }
        private static enum MapCount{map_count,map_counts}
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] word = line.split("\t");
            //定义计数器1
            Counter counter = context.getCounter("map_couter", "map");
            counter.increment(1);
            //定义计数器2

            context.getCounter(MapCount.map_count).increment(1);
            context.write(new Text(line+"\t"+hsMap.get(word[1])),NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, IOException, URISyntaxException {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapPhone.class);
        job.setMapperClass(mymap.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\数据文件\\ReducerJoinInput\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\MapJoinOut"));

        job.setNumReduceTasks(0);

        job.addCacheFile(new URI("file:///E:/数据文件/ReducerJoinInput/pd.txt"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
