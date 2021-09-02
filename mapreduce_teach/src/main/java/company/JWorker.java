package company;


import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
* 列出名字以 J 开头的员工姓名及其所属部门名称
*
* 求名字以 J 开头的员工姓名机器所属部门名称，只需判断员工姓名是否以 J 开头。
* 首先和问题 1 类似在 Mapper 的 Setup 阶段缓存部门数据，然后在 Mapper 阶段判断员工姓名是否以 J 开头，
* 如果是抽取出员工姓名和员工所在部门编号，利用缓存部门数据把部门编号对应为部门名称，转换后输出结果。
* */
public class JWorker extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //读取缓存文件
            URI[] cacheFiles = context.getCacheFiles();
            String path = cacheFiles[0].getPath();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            //判断每一行是否有数据
            String line;
            while (StringUtils.isNotEmpty(line=reader.readLine())){
                String[] split = line.split(",");
                deptMap.put(split[0],split[1]);
            }
            //关闭资源
            IOUtils.closeStream(reader);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // 输出员工姓名为J开头的员工信息，key为员工姓名和value为员工所在部门名称
            if (kv[1].toString().trim().startsWith("J")) {
                context.write(new Text(kv[1].trim()), new Text(deptMap.get(kv[7].trim())));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job =Job.getInstance();

        // 设置Mapper和Reduce类
        job.setJarByClass(JWorker.class);
        job.setMapperClass(MapClass.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
        job.addCacheFile(new URI("file:///E:/%E6%95%B0%E6%8D%AE%E6%96%87%E4%BB%B6/Dept_Emp/dept.txt"));
        FileInputFormat.addInputPath(job, new Path("E:\\数据文件\\Dept_Emp\\emp.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\dept_emp_out"));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JWorker(), args);
        System.exit(res);
    }
}