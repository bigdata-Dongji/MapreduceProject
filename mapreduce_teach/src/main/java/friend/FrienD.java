package friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
//社交好友推荐算法
public class FrienD {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FrienD.class);
        job.setMapperClass(friend_map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(friend_reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\数据文件\\FriendInput\\friend.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\FriendOut"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
    /*
    map结果：
    A B
    B A
    C D
    D C
    E F
    F E
    F G
    G F
    B D
    D B
    B C
    C B
    */
    public static class friend_map extends Mapper<Object, Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            context.write(new Text(split[0]),new Text(split[1]));
            context.write(new Text(split[1]),new Text(split[0]));
        }
    }
    /*
    map之后，Shuffling将相同key的整理在一起，结果如下：
    shuffling结果(将结果输出到reduce)：
    A B

    B A
    B D
    B C

    C D
    C B

    E F

    F E
    F G

    G F
    */
    //reduce将上面的数据进行笛卡尔积计算
    public static class friend_reduce extends Reducer<Text, Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<>();
            for (Text v: values) {
                set.add(v.toString());
            }
            if (set.size()>1){
                //hasNext():判断当前元素是否存在，并没有指向的移动
                //next():返回当前元素， 并指向下一个元素
                //set中的元素在内存中存放的时候并不连续，而Iterator就像是指针一样，hasNext（）就能找到它的下一个元素
               for (Iterator<String> i = set.iterator();i.hasNext();){
                    String name=i.next();
                    for (Iterator<String> j = set.iterator();j.hasNext();){
                        String copyname=j.next();
                        if (!name.equals(copyname)){
                            context.write(new Text(name),new Text(copyname));
                        }
                    }
               }
            }
        }
    }
}
