package ReducerJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReducePhone {
    public static class TBean implements Writable{

        private String id;
        private String pid;
        private int count;
        private String name;
        private String flag;

        public TBean() {
        }

        public TBean(String id, String pid, int count, String name, String flag) {
            this.id = id;
            this.pid = pid;
            this.count = count;
            this.name = name;
            this.flag = flag;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(id);
            dataOutput.writeUTF(pid);
            dataOutput.writeInt(count);
            dataOutput.writeUTF(name);
            dataOutput.writeUTF(flag);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            id=dataInput.readUTF();
            pid=dataInput.readUTF();
            count=dataInput.readInt();
            name=dataInput.readUTF();
            flag=dataInput.readUTF();
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getPid() {
            return pid;
        }

        public void setPid(String pid) {
            this.pid = pid;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getFlag() {
            return flag;
        }

        public void setFlag(String flag) {
            this.flag = flag;
        }

        @Override
        public String toString() {
            return id + "\t" + count + "\t" + name ;
        }
    }

    public static class mymap extends Mapper<Object, Text,Text,TBean>{
        String name;
        TBean tb=new TBean();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取输入文件名称
            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            name=inputSplit.getPath().getName();
//            FileSplit inputSplit1 = (FileSplit) context.getInputSplit();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] word = line.split("\t");
            if (name.startsWith("order")){
                tb.setId(word[0]);
                tb.setPid(word[1]);
                tb.setCount(Integer.parseInt(word[2]));
                tb.setName("");
                tb.setFlag("order");
                context.write(new Text(word[1]),tb);
            }else {
                tb.setId("");
                tb.setPid(word[0]);
                tb.setCount(0);
                tb.setName(word[1]);
                tb.setFlag("pd");
                context.write(new Text(word[0]),tb);
            }
        }
    }
    public static class  myreducer extends Reducer<Text,TBean,TBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<TBean> values, Context context) throws IOException, InterruptedException {
            //存放bean集合
            ArrayList<TBean> list = new ArrayList<>();
            TBean pdtb = new TBean();

            for (TBean tb:values){
                if ("order".equals(tb.getFlag())){
                    TBean tmp=new TBean();
                    try {
                        //copyTBean属性，防止数据丢失
                        BeanUtils.copyProperties(tmp,tb);
                        //放入list集合
                        list.add(tmp);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else {
                    try {
                        BeanUtils.copyProperties(pdtb,tb);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }

            for (TBean tb:list) {
                tb.setName(pdtb.getName());
                context.write(tb,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReducePhone.class);
        job.setMapperClass(mymap.class);
        job.setReducerClass(myreducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TBean.class);

        job.setOutputKeyClass(TBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\数据文件\\ReducerJoinInput\\*"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\数据文件\\out\\ReduceJoinOut"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
