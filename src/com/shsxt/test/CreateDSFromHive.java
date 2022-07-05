package com.shsxt.test;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;


public class CreateDSFromHive {
    //-Xms800m -Xmx800m  -XX:PermSize=64M -XX:MaxNewSize=256m -XX:MaxPermSize=128m
	public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("jsonfile")
                .master("local")
                //开启hive的支持，接下来就可以操作hive表了
                // 前提需要是需要开启hive metastore 服务
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql("USE traffic");
        sparkSession.sql("DROP TABLE IF EXISTS monitor_flow_action");
		//在hive中创建student_infos表

        sparkSession.sql("CREATE TABLE IF NOT EXISTS monitor_flow_action (date STRING,age INT) row format delimited fields terminated by '\t' ");
        sparkSession.sql("load data local inpath './data/student_infos' into table student_infos");
        //注意：此种方式，在ide中需要能读取到数据，同时也要能读取到 metastore服务的配置信息。
        //将idea的conf目录设置成resource资源目录，就可以加载到配置
        sparkSession.sql("DROP TABLE IF EXISTS student_scores");
        sparkSession.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by '\t'");
        sparkSession.sql("LOAD DATA "
				+ "LOCAL INPATH './data/student_scores'"
				+ "INTO TABLE student_scores");



//		Dataset<Row> df = hiveContext.table("student_infos");//读取Hive表加载Dataset方式
        /**
         * 查询表生成Dataset
         */
		Dataset<Row> goodStudentsDF = sparkSession.sql("SELECT si.name, si.age, ss.score "
				+ "FROM student_infos si "
				+ "JOIN student_scores ss "
				+ "ON si.name=ss.name "
				+ "WHERE ss.score>=80");

		goodStudentsDF.registerTempTable("goodstudent");
        Dataset<Row> result = sparkSession.sql("select * from goodstudent");
		result.show();

		/**
		 * 将结果保存到hive表 good_student_infos
		 */
        sparkSession.sql("DROP TABLE IF EXISTS good_student_infos");
		goodStudentsDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");

        sparkSession.stop();
	}
}
