import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TwoSort1 {
    public static class SortBean implements WritableComparable<SortBean> {
        private int first;
        private int scend;

        @Override
        public String toString() {
            return first + "\t" + scend;
        }

        public SortBean() {
        }

        public SortBean(int first, int scend) {
            this.first = first;
            this.scend = scend;
        }

        public int getFirst() { return first; }

        public void setFirst(int first) {
            this.first = first;
        }

        public int getScend() {
            return scend;
        }

        public void setScend(int scend) {
            this.scend = scend;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(first);
            dataOutput.writeInt(scend);
        }

        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readInt();
            scend = dataInput.readInt();
        }


        @Override
        public int compareTo(SortBean o) {
            if (first!=o.first){
                return first<o.first?-1:1;
            }else if (scend!=o.scend){
                return scend<o.scend?-1:1;
            }else return 0;
        }

//        @Override
//        public int hashCode() {
//            return first*157+scend;
//        }
//
//        @Override
//        public boolean equals(Object obj) {
//            if (obj==null){
//                return false;
//            }
//            if (this==obj){
//                return true;
//            }
//            if (obj instanceof SortBean){
//                SortBean sortBean= (SortBean) obj;
//                return sortBean.first==first &sortBean.scend==scend;
//            }else return false;
//        }

    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        FileSystem fileSystem = FileSystem.get(new URI("file:///E:/数据文件/out/TwoSort_out"), new Configuration());
        if (fileSystem.exists(new Path("E:\\数据文件\\out\\TwoSort_out"))) {
            fileSystem.delete(new Path("E:\\数据文件\\out\\TwoSort_out"), true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(TwoSort1.class);
        job.setMapperClass(TwoSort_Map.class);
        job.setReducerClass(TwoSort_Reduce.class);
        job.setPartitionerClass(par.class);
        job.setGroupingComparatorClass(group.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("E:\\数据文件\\goods_visit2"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\TwoSort_out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TwoSort_Map extends Mapper<Object, Text, SortBean, NullWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            SortBean sortBean = new SortBean();
            String[] lines = value.toString().split(" ");
            sortBean.setFirst(Integer.parseInt(lines[1]));
            sortBean.setScend(Integer.parseInt(lines[0]));
            context.write(sortBean, NullWritable.get());

        }
    }

    public static class par extends Partitioner<SortBean,NullWritable>{

        @Override
        public int getPartition(SortBean sortBean, NullWritable nullWritable, int i) {
            return Math.abs(sortBean.getFirst()*127)%i;
        }
    }

    public static class group extends WritableComparator{
        public group() {super(SortBean.class,true);}

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SortBean s1= (SortBean) a;
            SortBean s2= (SortBean) b;
            int l=s1.getFirst();
            int r=s2.getFirst();
            return l==r?0:(l<r?-1:1);
        }
    }

    public static class TwoSort_Reduce extends Reducer<SortBean, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), NullWritable.get());
        }
    }
}
