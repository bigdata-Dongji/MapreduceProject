package mapreduceProgram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.print.attribute.standard.JobName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduceProgram.FlowSort.MySortKey;
import mapreduceProgram.FlowSort.SortMapper;
import mapreduceProgram.FlowSort.SortReducer;
import sun.tools.tree.SuperExpression;

public class GroupMax {

	public static class Pair implements WritableComparable<Pair> {
		private String order_id;
		private DoubleWritable amount;

		public Pair() {
			// TODO Auto-generated constructor stub
		}

		public Pair(String id, DoubleWritable amount) {
			this.order_id = id;
			this.amount = amount;
		}

		public String getOrder_id() {
			return order_id;
		}

		public void setOrder_id(String order_id) {
			this.order_id = order_id;
		}

		public DoubleWritable getAmount() {
			return amount;
		}

		public void setAmount(DoubleWritable amount) {
			this.amount = amount;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(order_id);
			out.writeDouble(amount.get());
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			order_id = in.readUTF();
			amount = new DoubleWritable(in.readDouble());
		}

		@Override
		public int compareTo(Pair o) {
			if (order_id.equals(o.order_id)) {// ??????order_id?????????amount????????????
				return o.amount.compareTo(amount);
			} else {
				return order_id.compareTo(o.order_id);
			}
		}

	}

	public static class MyMapper extends Mapper<Object, Text, Pair, NullWritable> {
		Pair pair = new Pair();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			pair.setOrder_id(strs[0]);
			pair.setAmount(new DoubleWritable(Double.parseDouble(strs[2])));
			context.write(pair, NullWritable.get());
			System.out.println(pair.getOrder_id()+","+pair.getAmount());
		}
	}

	public static class MyReducer extends Reducer<Pair, NullWritable, Text, DoubleWritable> {
		public void reduce(Pair key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.getOrder_id()), key.getAmount());
			System.out.println(key.order_id+": "+key.amount.get());
			// ????????????????????????????????????
//			for (NullWritable value : values) {
//				context.write(new Text(key.getOrder_id()), key.getAmount());
//				System.out.println(key.order_id+": "+key.amount.get());
//			}
		}
	}
	// ????????????????????????????????????????????????????????????reduce??????????????????????????????Reduce??????????????????????????????
	// ??????????????????
	public static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			// TODO Auto-generated constructor stub
			super(Pair.class, true);
		}
		// Mapper?????????Pair????????????????????????????????????Pair??????order_id??????
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			Pair oa = (Pair) a;
			Pair ob = (Pair) b;
			return oa.getOrder_id().compareTo(ob.getOrder_id());
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// ??????HDFS????????????
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		// ??????job????????????
		String JobName = "GroupMax";
		Job job = Job.getInstance(conf, JobName);
		job.setJarByClass(GroupMax.class);
		job.setJar("export\\GroupMax.jar");
		// Mapper
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(NullWritable.class);
		// Reducer
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(DoubleWritable.class);
		// GroupComparator??????????????????
		job.setGroupingComparatorClass(GroupComparator.class);
		// ????????????????????????
		String dataDir = "/workspace/data/orderDetail.txt"; // ????????????
		String outputDir = "/workspace/groupMax/output"; // ??????????????????
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		// ????????????
		System.out.println("Job: "+JobName+" is running...");
		if (job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}

}
