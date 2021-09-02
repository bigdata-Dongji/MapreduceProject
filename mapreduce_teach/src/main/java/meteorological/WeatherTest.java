package meteorological;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WeatherTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WeatherTest.class);
        job.setMapperClass(WeatherMap.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(WeatherReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Weather.class);
        job.setNumReduceTasks(3);
        job.setPartitionerClass(Auto.class);
        Path inPath = new Path("E:\\数据文件\\meteorologicaldata\\a.txt");
        Path out = new Path("E:\\数据文件\\out\\meteorologicaldata_out");
        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, out);
        job.waitForCompletion(true);
    }
}
