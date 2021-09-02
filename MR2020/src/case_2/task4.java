package case_2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class task4 {
    private static int sum=0;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setMapperClass(task4_Map.class);
        job.setReducerClass(task4_Reduce.class);
        job.setMapOutputKeyClass(Produce.class);
        job.setMapOutputValueClass(NullWritable.class);
        Path in=new Path("E:\\数据文件\\out\\case_2_task1_out\\part-r-00000");
        Path out=new Path("E:\\数据文件\\out\\case_2_task4_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class Produce implements WritableComparable<Produce> {
        private String id;
        private String title;
        private double popularity;
        private double price;

        @Override
        public int hashCode() {
            return id.hashCode()+title.hashCode();
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public void setPopularity(double popularity) {
            this.popularity = popularity;
        }

        public void setPrice(double price) {
            this.price = price;
        }


        @Override
        public int compareTo(Produce o){
            if (popularity!=o.popularity){
                return popularity<o.popularity?1:-1;
            }else if (price!=o.price)return price<o.price?1:-1;
            else return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(id);
            dataOutput.writeUTF(title);
            dataOutput.writeDouble(popularity);
            dataOutput.writeDouble(price);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            id=dataInput.readUTF();
            title=dataInput.readUTF();
            popularity=dataInput.readDouble();
            price=dataInput.readDouble();
        }
    }

    public static class task4_Map extends Mapper<Object, Text,Produce, NullWritable>{
        private static final Produce produce=new Produce();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines=value.toString().split(",");
            produce.setId(lines[0]);
            produce.setTitle(lines[2]);
            produce.setPopularity(Double.parseDouble(lines[3]));
            try{
                produce.setPrice(Double.parseDouble(lines[5]));
            }catch (NumberFormatException e){return;}
            context.write(produce,NullWritable.get());
        }
    }
    public static class task4_Reduce extends Reducer<Produce,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Produce key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            sum++;
            if (sum<101){
                context.write(new Text(key.id+"\t"+key.title+'\t'+key.popularity+'\t'+key.price),NullWritable.get());
            }
        }
    }
}
