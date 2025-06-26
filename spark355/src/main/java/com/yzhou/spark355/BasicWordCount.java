package com.yzhou.spark355;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

public final class BasicWordCount {
  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(BasicWordCount.class);
  //private static final Logger logger = parentLogger;

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    // 默认读取当前目录下的wc_input.txt文件
    String fileUrl = "wc_input.txt";

    if (args.length == 1) {
      fileUrl = args[0];
    }

    // 1 创建SparkSession
    SparkSession spark = SparkSession
            .builder()
            // 发布的时候需要注释掉
            .master("local")
            .appName("BasicWordCount")
            .getOrCreate();

    JavaRDD<String> lines;

    // 2 读取文件
    if (fileUrl.startsWith("http")) {
      // 2.1 从http获取文件
      URL url = new URL(fileUrl);
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
      //设置超时间为3秒
      conn.setConnectTimeout(3*1000);

      //得到输入流
      InputStream inputStream = conn.getInputStream();
      //获取字节数组
      byte[] bytes = readInputStream(inputStream);
      String content = new String(bytes);
      inputStream.close();

      JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
      lines = sc.parallelize(Arrays.asList(content.split("\r\n")), 1);
    } else {
      // 2.2 从本地获取文件
      lines = spark.read().textFile(fileUrl).javaRDD();
    }

    // 3 逐行分词
    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    // 4 构建单词Tuple
    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    // 5 统计单词出现次数
    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    // 6 收集统计结果
    List<Tuple2<String, Integer>> output = counts.collect();

    // 7 打印结果
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
      logger.info(tuple._1() + ": " + tuple._2());
    }

    // 睡眠10s，用于观察Pod的变化
    logger.info("sleep 10s");
    Thread.sleep(10*1000);

    spark.stop();
  }

  /**
   * 从输入流中获取字节数组
   * @param inputStream
   * @return
   * @throws IOException
   */
  public static  byte[] readInputStream(InputStream inputStream) throws IOException {
    byte[] buffer = new byte[1024];
    int len = 0;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    while((len = inputStream.read(buffer)) != -1) {
      bos.write(buffer, 0, len);
    }
    bos.close();
    return bos.toByteArray();
  }
}