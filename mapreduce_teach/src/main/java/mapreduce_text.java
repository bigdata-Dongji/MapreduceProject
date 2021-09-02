import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class    mapreduce_text {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost/movie",
                "root",
                "root");

        Job job = Job.getInstance(conf);
        job.setJarByClass(mapreduce_text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path("E:\\数据文件\\buyer_favorite1"));
//        DBInputFormat;

        DBOutputFormat.setOutput(job,"text", new String[]{"word", "num"});
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class KeyWordTable implements Writable, DBWritable{
        private String keyword;
        private int sum;

        //构造有参函数
        public KeyWordTable(String keyword,int sum){
            this.keyword=keyword;
            this.sum=sum;
        }

        public int getSum() {
            return sum;
        }

        public String getKeyword() {
            return keyword;
        }

        public void setKeyword(String keyword) {
            this.keyword = keyword;
        }

        public void setSum(int sum) {
            this.sum = sum;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.sum);
            dataOutput.writeUTF(this.keyword);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.sum=dataInput.readInt();
            this.keyword=dataInput.readUTF();
        }

        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1,this.keyword);
            preparedStatement.setInt(2,this.sum);

        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.keyword=resultSet.getString(1);
            this.sum=resultSet.getInt(2);
        }
    }
    public static class Map extends Mapper<Object, Text,Text, IntWritable> {
        private static final Text newkey=new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keys=value.toString().split("\t");
            newkey.set(keys[0]);
            context.write(newkey,new IntWritable(1));
        }
    }
    public static class Reduce extends Reducer<Text,IntWritable,KeyWordTable,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable value:values){
                sum+=value.get();
            }
            KeyWordTable keyWordTable = new KeyWordTable(key.toString(),sum);
            context.write(keyWordTable,NullWritable.get());
        }
    }
}
