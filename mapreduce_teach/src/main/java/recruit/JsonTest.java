package recruit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
//import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;

public class JsonTest {

    public static void main(String[] args) throws Exception{
        //写入HBase表
//        Configuration config = HBaseConfiguration.create();
//        //设置zookeeper的配置
//        config.set("hbase.zookeeper.quorum", "127.0.0.1");
//        Connection connection = ConnectionFactory.createConnection(config);
//        Admin admin = connection.getAdmin();
//        TableName tableName = TableName.valueOf("job");
//        boolean isExists = admin.tableExists(tableName);
//        if (!isExists) {
//            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
//            ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();// 构建列族对象
//            tableDescriptor.setColumnFamily(family); // 设置列族
//            admin.createTable(tableDescriptor.build()); // 创建表
//        } else {
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
//            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
//            ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();// 构建列族对象
//            tableDescriptor.setColumnFamily(family); // 设置列族
//            admin.createTable(tableDescriptor.build()); // 创建表
//        }
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost/mapreducedb",
                "root",
                "511722");
        Job job = Job.getInstance(conf);
        job.setJarByClass(JsonTest.class);
        job.setMapperClass(JsonMap.class);
        job.setReducerClass(JsonReduce.class);

        job.setMapOutputKeyClass(Jsonrecruit.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("E:\\数据文件\\recruitdata\\data.json"));
        String[] fields = new String[]{"id", "company_name","eduLevel_name","emplType","jobName","salary","createDate","endDate","city_name","companySize","welfare","responsibility","place","workingExp"};
//        DBInputFormat.setInput();
        DBOutputFormat.setOutput(job,"job",fields);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
