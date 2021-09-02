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

public class task5 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setMapperClass(task5_Map.class);
        job.setReducerClass(task5_Reduce.class);
        job.setMapOutputKeyClass(Product.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path in=new Path("E:\\数据文件\\out\\case_2_task1_out\\part-r-00000");
        Path out=new Path("E:\\数据文件\\out\\case_2_task5_out");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static int sum=0;
    public static class Product implements WritableComparable<Product> {
        private String id;
        private String title;
        private double comment;

        public void setId(String id) {
            this.id = id;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public void setComment(double comment) {
            this.comment = comment;
        }

        @Override
        public int hashCode() {
            return id.hashCode()+title.hashCode();
        }

        @Override
        public int compareTo(Product o) {
            if (comment!=o.comment){
                return comment<o.comment?1:-1;
            }else return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(id);
            dataOutput.writeUTF(title);
            dataOutput.writeDouble(comment);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            id=dataInput.readUTF();
            title=dataInput.readUTF();
            comment=dataInput.readDouble();
        }
    }

    public static class task5_Map extends Mapper<Object, Text,Product,NullWritable> {
        private static final Product product=new Product();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(",");
            product.setId(lines[0]);
            product.setTitle(lines[2]);
            product.setComment(Double.parseDouble(lines[7])/(Double.parseDouble(lines[8])/100));
            context.write(product,NullWritable.get());
        }
    }

    public static class task5_Reduce extends Reducer<Product,NullWritable,Text,NullWritable> {
        @Override
        protected void reduce(Product key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            sum++;
            if (sum<101) {
                context.write(new Text(key.id+"\t"+key.title+'\t'+Math.ceil(key.comment)),NullWritable.get());
            }

        }
    }
}
