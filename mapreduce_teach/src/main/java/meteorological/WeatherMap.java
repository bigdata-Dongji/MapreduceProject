package meteorological;

import java.io.*;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.sound.midi.Soundbank;

/*数据说明
* sky.txt:
* 1,积云
* 天气情况	cumulus
*
* a.txt:
* 2005	01	01	16	-6	-28	10157	260	31	8	0	-9999
* 年	月	日	小时	温度	湿度	气压	风向	风速	天气情况	1h降雨量	6h降雨量
*
* a.txt数据切分方式：一个或多个空格；
* sky.txt数据切分方式：逗号；
*
*
* 清洗条件
* 将分隔符转化为逗号；
* 清除不合法数据：字段长度不足，风向不在[0,360]的，风速为负的，气压为负的，天气情况不在[0,10]，
* 湿度不在[0,100]，温度不在[-40,50]的数据；
* 将a.txt与sky.txt的数据以天气情况进行join操作，把天气情况变为其对应的云属；
* 对进入同一个分区的数据排序； 排序规则： （1）同年同月同天为key； （2）按每日温度升序；
* （3）若温度相同则按风速升序； （4）风速相同则按压强降序。
* */
public class WeatherMap extends Mapper<LongWritable,Text,Weather,NullWritable>{

    private HashMap<String,String> map=new HashMap<String,String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        File file = new File("E:\\数据文件\\meteorologicaldata\\sky.txt");
//        URI[] cacheFiles = context.getCacheFiles();
//        String path = cacheFiles[0].getPath();
//        new BufferedReader(new InputStreamReader(new FileInputStream()))
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
//        BufferedReader reader1 = new BufferedReader(new FileReader("E:\\数据文件\\out\\case_2_task4_out\\part-r-00000"));
        //判断每一行是否有数据
        String line;
//        StringUtils.isNotEmpty(line=reader.readLine())
        while (StringUtils.isNotEmpty(line=reader.readLine())){
            String[] split = line.split(",");
            map.put(split[0],split[1]);
        }
        //关闭资源
//        reader.close();
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\s+");    //一个或多个空格切分方式："\\s+"
        //清除不合法数据:字段长度不足
        if (split.length!=12){return;}
        //风向不在[0,360]的
        if (Integer.parseInt(split[7])<0 || Integer.parseInt(split[7])>360){return;}
        //风速为负的，气压为负的
        int pressure=Integer.parseInt(split[6]);
        int wind_speed=Integer.parseInt(split[8]);
        if (pressure<0 || wind_speed<0){return;}
        //天气情况不在[0,10]
        if (Integer.parseInt(split[9])<0 || Integer.parseInt(split[9])>10){return;}
        //湿度不在[0,100]
        if (Integer.parseInt(split[5])<0 || Integer.parseInt(split[5])>100){return;}
        //温度不在[-40,50]
        int temperature=Integer.parseInt(split[4]);
        if (temperature<-40 || temperature>50){return;}
        //把天气情况变为其对应的云属
        String sky_condition=map.get(split[9]);
        Weather weather = new Weather(split[0],split[1],split[2],split[3],temperature,split[5],pressure,split[7],wind_speed,sky_condition,split[10],split[11]);
        context.write(weather,NullWritable.get());
    }
}
