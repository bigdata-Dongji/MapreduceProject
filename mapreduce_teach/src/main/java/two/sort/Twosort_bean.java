package two.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Twosort_bean {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Twosort_bean.class);

        job.setMapperClass(mymap.class);
        job.setReducerClass(myreduce.class);

        job.setMapOutputKeyClass(sorttwo.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(sorttwo.class);
        job.setOutputValueClass(NullWritable.class);

//        job.setGroupingComparatorClass(ordergroup.class);

        FileInputFormat.addInputPath(job, new Path("E:\\数据文件\\goods_visit2"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\TwoSort_out_group"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class sorttwo implements WritableComparable<sorttwo> {

        private int id;
        private int money;

        public sorttwo() {
            super();
        }

        public sorttwo(int id, int money) {
            this.id = id;
            this.money = money;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public float getMoney() {
            return money;
        }

        public void setMoney(int money) {
            this.money = money;
        }


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(id);
            dataOutput.writeFloat(money);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            id = dataInput.readInt();
            money = dataInput.readInt();
        }

        @Override
        public String toString() {
            return  id + "\t" + money ;
        }

        @Override
        public int compareTo(sorttwo o) {
            int result;
            if (id > o.getId()) {
                result = 1;
            } else if (id < o.getId()) {
                result = -1;
            } else {
                if (money > o.getMoney()) {
                    result = -1;
                } else if (money < o.getMoney()) {
                    result = 1;
                } else {
                    result = 0;
                }
            }
            return result;
        }
    }

    public static class mymap extends Mapper<LongWritable, Text,sorttwo, NullWritable>{
        sorttwo sort = new sorttwo();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            sort.setId(Integer.parseInt(split[1]));
            sort.setMoney(Integer.parseInt(split[0]));
            context.write(sort,NullWritable.get());
        }

//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//        }
    }
    public static class myreduce extends Reducer<sorttwo, NullWritable,sorttwo, NullWritable> {
        @Override
        protected void reduce(sorttwo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

//    public static class ordergroup extends WritableComparator{
//
//        protected ordergroup() {
//            super(sorttwo.class,true);
//        }
//
//
//        @Override
//        public int compare(WritableComparable a, WritableComparable b) {
//
//            sorttwo asort= (sorttwo) a;
//            sorttwo bsort= (sorttwo) b;
//            int result;
//            if (asort.getId()>bsort.getId()){
//                result=1;
//            }else if (asort.getId()<bsort.getId()){
//                result=-1;
//            }else {
//                result=0;
//            }
//            return result;
//        }
//    }
}
