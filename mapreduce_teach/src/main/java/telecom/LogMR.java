package telecom;


import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**MapReduce操作类*/
public class LogMR {
    // 定义两个Map 存放数据库查询到的数据
    static Map<String,String> userMap = new HashMap<String, String>();
    static Map<String,String> addressMap = new HashMap<String, String>();

    static class MyMapper extends Mapper<LongWritable, Text, PhoneLog, NullWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{

            //获取连接对象
            Connection conn = DBHelper.getConn();

            //定义执行的sql语句，需要做的操作
            String userSQL = "SELECT * FROM userphone";
            try {
                /*
                * 一般的使用习惯Select语句使用executeQuery()方法执行，Delete、Update、Insert语句使用executeUpdate()方法执行，
                * 而Create和Drop语句使用execute()方法执行，当然也可以使用executeUpdate()方法。
                * PreparedStatement接口提供了三种执行 SQL 语句的方法：executeQuery、executeUpdate 和 execute。
                *
                * PreparedStatement是预编译的,对于批量处理可以大大提高效率. 也叫JDBC存储过程
                *
                *  结果集(ResultSet)是数据中查询结果返回的一种对象，可以说结果集是一个存储查询结果的对象，
                *  但是结果集并不仅仅具有存储的功能，他同时还具有操纵数据的功能，可能完成对数据的更新等.
                * */
                //通过连接对象将命令运输到数据库
                PreparedStatement userStatement = conn.prepareStatement(userSQL);//接收sql语句
                ResultSet resultSet = userStatement.executeQuery();//执行并返回数据
                while (resultSet.next()){//遍历ResultSet集合
                    //resultSet的方法getString，获取数据，可以指定列名，列索引（列号）
                    String number = resultSet.getString(2);//指定列号
                    String trueName = resultSet.getString(3);
                    userMap.put(number,trueName);
                }
                //关闭资源对象使用的顺序，倒序
                resultSet.close();
                userStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            String addressSQL = "SELECT * FROM allregion";
            try {
                PreparedStatement addressStatement = conn.prepareStatement(addressSQL);
                ResultSet resultSet = addressStatement.executeQuery();
                while (resultSet.next()){
                    String codeNum = resultSet.getString(2);
                    String address = resultSet.getString(3);
                    addressMap.put(codeNum,address);
                }
                //关闭资源对象使用的顺序，倒序
                resultSet.close();
                addressStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            //关闭连接资源

            DBHelper.closeConnection();

        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            * 数据清洗工作
            *
            * 15733218050,15778423030,1542457633,1542457678,450000,530000
            * 呼叫者手机号	接受者手机号	开始时间戳（s）	接受时间戳（s）	呼叫者地址省份编码	接受者地址省份编码
            * 邓二,张倩,13666666666,15151889601,2018-03-29 10:58:12,2018-03-29 10:58:42,30,黑龙江省,上海市
            * 户名A	用户名B	用户A的手机号	用户B的手机号	开始时间	结束时间	通话时长	用户A地理位置	用户B地理位置
            *
            * */
            if (value.toString().contains("手机号")){return;}  //去除第一行字段名
            String[] lines = value.toString().split(",");
            String userA = userMap.get(lines[0]);
            String userB = userMap.get(lines[1]);
            String userANumber = lines[0];
            String userBNumber = lines[1];
            //秒级时间戳转日期时间，先转成毫秒级时间戳，在转成日期时间
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String startTimeStramp = lines[2];
            String endTimeStramp = lines[3];
            //先转成毫秒级时间戳
            Long startTimeLong = Long.parseLong(startTimeStramp) * 1000;
            Long endTimeLong = Long.parseLong(endTimeStramp) * 1000;
            // 将开始时间和结束时间戳转成 日期时间
            String startTime = dateFormat.format(new Date(startTimeLong));
            String endTime = dateFormat.format(new Date(endTimeLong));
            //计算通信时长(以秒为单位)
            Long useTime = endTimeLong/1000 - startTimeLong/1000;

            String addressA = addressMap.get(lines[4]);
            String addressB = addressMap.get(lines[5]);
            PhoneLog phoneLog = new PhoneLog();
            phoneLog.SetPhoneLog(userA,userB,userANumber,userBNumber,startTime,endTime,useTime,addressA,addressB);
            context.write(phoneLog,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {   //Exception是所有异常的祖宗
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LogMR.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(PhoneLog.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        Path in = new Path("E:\\数据文件\\telecomdata\\a.txt");
        Path out = new Path("E:\\数据文件\\out\\telecom_out");
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.waitForCompletion(true);
    }


}