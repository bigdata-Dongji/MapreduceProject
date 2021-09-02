import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object scalaWordCount {
  def main(args: Array[String]): Unit = {
    //设定spark计算框架的运行环境
    val conf =new SparkConf()
    conf.setMaster("local").setAppName("scalawordcount")
    //创建一个spark上下文对象
    val sc = new SparkContext(conf)
    //读取文件
    val lines = sc.textFile("E:\\数据文件\\buyer_favorite1")
    //将数据进行结构化转换
    val words: RDD[(String, Int)] = lines.map(line => (line.split("\t")(0), 1))
    //将结构化数据进行分组聚合
    val wordsToSum: RDD[(String, Int)] = words.reduceByKey(_ + _)
    //保存文件,设置一个分区既保存成一个文件
    wordsToSum.coalesce(1,true).saveAsTextFile("E:\\数据文件\\out\\spark_out")
    //停止spark对象
    sc.stop()
  }
}
