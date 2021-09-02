package job;

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
        //定义工作的job
        Job job=Job.getInstance();
        //job工作的java类
        job.setJarByClass(task1.class);
        //指定job工作的map类
        job.setMapperClass(task1_Map.class);
        //指定map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //指点map输出的value类型
        job.setMapOutputValueClass(NullWritable.class);
        //定义输入输出的文件路径
        Path in=new Path("E:\\数据文件\\51jobs.json");
        Path out=new Path("E:\\数据文件\\out\\job_task1_out");
        //增加清洗文件来源路径
        FileInputFormat.addInputPath(job,in);
        //设置输出文件路径
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class task1_Map extends Mapper<Object, Text,Text,NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //清洗每条json最后面的逗号，因为json对象只需要完整的{}，substring取字符串
            String word=value.toString().substring(0,value.toString().length()-1);
            //将每条json{}数据转换成json对象
            JSONObject jsonObject= JSON.parseObject(word);
            //通过json对象的键取json对象的值
            String job_name = jsonObject.getString("job_name");
            String company_name = jsonObject.getString("company_name");
            String providesalary_text = jsonObject.getString("providesalary_text");
            String attribute_text = jsonObject.getString("attribute_text");
            String companytype_text = jsonObject.getString("companytype_text");
            //"attribute_text": "['长沙'; '1年经验'; '本科'; '招1人']"
            //将每条json的attribute_text，切分
            String[] line=attribute_text.split("; ");
            //定义这些字段为不限，用来替换空值
            String jinyan="不限";
            String education = "不限";
            String num = "不限";
            //循环切分的attribute_text，通过判断来进行赋值，为空的不变，就是上面的‘不限’，contains字符串包含
            for (String i:line){
                if (i.contains("经验")){
                    jinyan = i.replace("'","");
                }
                if (i.contains("招")){
                    num = i.replace("]","").replace("'","");
                }
                if ("初中中专高中大专硕士博士本科".contains(i)){
                    education = i.replace("]","").replace("'","");
                }
            }
            //地址在第一位，且不变，都有
            String address = line[0].replace("[","").replace("'","");
            //StringBuilder是java的字符串序列，简单说就是字符串缓存区，可以将多个字符串放一起，然后tostring变成一个字符串
            //实例化StringBuilder对象

            StringBuilder stringBuilder=new StringBuilder();
            //往StringBuilder对象放入清洗好的数据，用逗号分隔
            stringBuilder.append(job_name);stringBuilder.append(",");
            stringBuilder.append(company_name);stringBuilder.append(",");
            stringBuilder.append(providesalary_text);stringBuilder.append(",");
            stringBuilder.append(companytype_text);stringBuilder.append(",");
            stringBuilder.append(address);stringBuilder.append(",");
            stringBuilder.append(jinyan);stringBuilder.append(",");
            stringBuilder.append(education);stringBuilder.append(",");
            stringBuilder.append(num);
            //将StringBuilder对象tostring变成字符串进行写入文件
            //因为不需要reduce操作，所以没有传入reduce，
            context.write(new Text(stringBuilder.toString()),NullWritable.get());
        }
    }
}
