package com.shsxt.spark.skynet;

import java.util.*;
import java.util.Map.Entry;

import com.shsxt.spark.constant.Constants;
import com.shsxt.spark.context.TrafficContext;
import com.shsxt.spark.dao.ICarTrackDAO;
import com.shsxt.spark.dao.ITaskDAO;
import com.shsxt.spark.dao.factory.DAOFactory;
import com.shsxt.spark.domain.CarTrack;
import com.shsxt.spark.domain.RandomExtractMonitorDetail;
import com.shsxt.spark.util.DateUtils;
import com.shsxt.spark.util.ParamUtils;
import com.shsxt.spark.util.StringUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;


import com.alibaba.fastjson.JSONObject;

import com.shsxt.spark.dao.IRandomExtractDAO;
import com.shsxt.spark.domain.RandomExtractCar;
import com.shsxt.spark.domain.Task;
import com.shsxt.spark.util.SparkUtils;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.internal.Trees;

/**
 * 抽取N辆车的信息
 *
 * @author root
 */
public class RandomExtractCars {


    public static void main(String[] args) {

        SparkSession sparkSession = TrafficContext.initContext("RandomExtractCars");

        //从配置文件中查询出来指定的任务ID

        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_EXTRACT_CAR);

        if (taskId == null) {
            System.err.println(" task id is null,exit...");
            return;
        }

        /**
         * 通过taskId从数据库中查询相应的参数
         * 	1、通过DAOFactory工厂类创建出TaskDAO组件
         * 	2、查询task
         */
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findTaskById(taskId);

        if (task == null) {
            return;
        }

        /**
         * task对象已经获取到，因为params是一个json，所以需要创建一个解析json的对象
         */
        JSONObject taskParamsJsonObject = JSONObject.parseObject(task.getTaskParams());

        /**
         * 统计出这一段时间内，所有卡口的信息，所以需要根据param参数，去monitor_flow_action临时表中获取结果
         */
        JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(sparkSession, taskParamsJsonObject);

        /**
         * 随机抽取N个车辆信息，比如一天有24个小时，其中08:00~09:00的车辆数量占当天总车流量的50%，在这天中我们需要随机抽取100个，
         * 那么08:00~09:00的，就得抽取100*50%=50，而且这50个需要随机抽取。
         * 我 们需要使用Spark自己实现一个算法，按照时间段分段抽取车辆信息，然后这些车辆信息可以很权威的代表整个城市的车辆信息，
         * 我们可以基于这些抽样的数据进行数据分析，可以绘制出这些车辆每天的运行轨迹，对于道路的规划起到了很重要的作用，
         * 比如，我们抽样出来的数据80%的车辆在早高峰和晚高峰都是基本同样的行车轨迹，然而他们每天途径的路段都会堵车，这时候我们可以根据这些数据对道路进行规划
         * 可以根据用户的画像进行多维度的数据分析
         *
         * 下面方法中将抽取出来的车辆信息插入到random_extract_car表中,将抽取的car的详细数据放入了random_extract_car_detail_info 表中
         * 返回了(car,Row)
         */
        JavaPairRDD<String, Row> randomExtractCar2DetailRDD = randomExtractCarInfo(sparkSession, taskId, taskParamsJsonObject, cameraRDD);

        /**
         * carTrackRDD<String,String>
         * k:car
         * v:date|carTracker
         * (car,"dateHour=2017-10-18|carTrack=monitor_id,monitor_id,monitor_id...")
         * 相同的车辆会出现在不同的时间段中，那么我们可以追踪在这个日期段中车辆的行驶轨迹
         */
        JavaPairRDD<String, String> carTrackRDD = getCarTrack(randomExtractCar2DetailRDD);


        /**
         * 将每一辆车的轨迹信息写入到数据库表car_track中
         */
        saveCarTrack2DB(taskId, carTrackRDD);

        sparkSession.close();
    }


    private static void saveCarTrack2DB(final long taskId, JavaPairRDD<String, String> carTrackRDD) {
        //action执行
        carTrackRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {

                //(car,"dateHour=2017-10-18|carTrack=monitor_id,monitor_id,monitor_id...")
                ICarTrackDAO carTrackDAO = DAOFactory.getCarTrackDAO();
                List<CarTrack> carTracks = new ArrayList<>();
                while (iterator.hasNext()) {
                    //(car,"dateHour=2017-10-18|carTrack=monitor_id,monitor_id,monitor_id...")
                    Tuple2<String, String> tuple = iterator.next();
                    String car = tuple._1;
                    String dateAndCarTrack = tuple._2;
                    String date = StringUtils.getFieldFromConcatString(dateAndCarTrack, "\\|", Constants.FIELD_DATE);
                    String track = StringUtils.getFieldFromConcatString(dateAndCarTrack, "\\|", Constants.FIELD_CAR_TRACK);
                    CarTrack carTrack = new CarTrack(taskId, date, car, track);
                    carTracks.add(carTrack);
                }
                //将车辆的轨迹存入数据库表car_track中
                carTrackDAO.insertBatchCarTrack(carTracks);
            }
        });

    }

    /**
     * 对抽取出来的car进行跟踪轨迹
     *
     * @param randomExtractCar2DetailRDD
     * @return (car,"dateHour=2017-10-18|carTrack=monitor_id, monitor_id, monitor_id...")
     */
    private static JavaPairRDD<String, String> getCarTrack(JavaPairRDD<String, Row> randomExtractCar2DetailRDD) {
        JavaPairRDD<String, Iterable<Row>> groupByCar = randomExtractCar2DetailRDD.groupByKey();

        JavaPairRDD<String, String> carTrackRDD = groupByCar.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                String car = tuple._1;
                Iterator<Row> carMetailsIterator = tuple._2.iterator();

                List<Row> rows = new ArrayList<>();
                while (carMetailsIterator.hasNext()) {
                    Row row = carMetailsIterator.next();
                    rows.add(row);
                }


                //按照卡扣拍摄的时间 action_time 来排序

                Collections.sort(rows, new Comparator<Row>() {

                    @Override
                    public int compare(Row r1, Row r2) {
                        if (DateUtils.after(r1.getString(4), r2.getString(4))) {
                            return 1;
                        }

                        return -1;
                    }
                });

                StringBuilder carTrack = new StringBuilder();
                String date = "";
                for (Row row : rows) {
                    carTrack.append("," + row.getString(1));
                    date = row.getString(0);
                }

                return new Tuple2<>(car, Constants.FIELD_DATE + "=" + date + "|" + Constants.FIELD_CAR_TRACK + "=" + carTrack.substring(1));
            }
        });

        return carTrackRDD;

    }


    /**
     * cameraRDD
     * 1、key：8-9	value：carCount    mapTopair   countByKey
     * 2、计算出来8-9的占全天总车流量的百分比
     * 3、Map<date,Map<hour,List<Interger>>>
     * 4、进行抽取
     *
     * @param sparkSession
     * @param taskId
     * @param params
     * @param cameraRDD
     * @return 抽取到的（car，row）
     * <p>
     * <p>
     * <p>
     * ("dateHour"="2017-10-10_08","car"="京X91427")
     * <p>
     * 1、date_hour key   car
     */
    private static JavaPairRDD<String, Row> randomExtractCarInfo(SparkSession sparkSession, final long taskId, JSONObject params, JavaRDD<Row> cameraRDD) {
        /**
         * key:时间段   value：car
         * dateHourCar2DetailRDD ---- ("dateHour"="2017-10-10_08","car"="沪X91427")
         */
        JavaPairRDD<String, String> dateHourCar2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String actionTime = row.getString(4);
                String dateHour = DateUtils.getDateHour(actionTime);//2017-12-09_08
                String car = row.getString(3);
                /**
                 * 为什么要使用组合Key？
                 *   	因为在某一个时间段内，这一两车很有可能经过多个卡扣
                 */
                String key = Constants.FIELD_DATE_HOUR + "=" + dateHour;
                String value = Constants.FIELD_CAR + "=" + car;

                return new Tuple2<>(key, value);
            }
        });


        /**
         * key-value <car,row>
         * car2DetailRDD ---- ("京X91427",Row)
         *
         */
        JavaPairRDD<String, Row> car2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {


            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String car = row.getString(3);
                return new Tuple2<>(car, row);
            }
        });

        /**
         * 相同的时间段内出现的车辆我们去重
         * key:时间段   value：car
         * dateHour2DetailRDD ---- ("dateHour"="2017-10-10_08","car"="京X91427")
         */
        JavaPairRDD<String, String> dateHour2DetailRDD = dateHourCar2DetailRDD.distinct();


        /**
         * String：dateHour
         * Object:去重后的这个小时段的总的车流量
         * (2018-08-08_16,500)
         * (2018-08-08_17,300)
         * (2018-08-09_17,300)
         */
        Map<String, Long> countByKey = dateHour2DetailRDD.countByKey();


        /**
         * 将<dateHour,car_count>这种格式改成格式如下： <date,<Hour,count>>
         * <2018-08-08,Map<10,500>>
         * <2018-08-09,Map<10,300>>
         * <2018-08-10,Map<11,400>>
         */
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<>();

//
        for (Entry<String, Long> entry : countByKey.entrySet()) {
            String dateHour = entry.getKey();//2017-10-10_08
            String[] dateHourSplit = dateHour.split("_");
            String date = dateHourSplit[0];
            String hour = dateHourSplit[1];
//            //本日期时段对应的车辆数
            Long count = entry.getValue();
//
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        /**
         * 要抽取的车辆数
         * 假设要抽取100辆车
         */

        String param = ParamUtils.getParam(params, Constants.FIELD_EXTRACT_NUM);

        int extractNums;

        if (param == null) {
            //如果参数为空，默认抽取50辆车
            extractNums = 50;
        } else {
            extractNums = Integer.parseInt(param);
        }


        /**
         * 一共抽取100辆车，平均每天应该抽取多少辆车呢？
         * extractNumPerDay = 100 ， dateHourCountMap.size()为有多少不同的天数日期，就是多长
         */
        int extractNumPerDay = extractNums / dateHourCountMap.size();

        /**
         * 记录每天每小时抽取索引的集合
         * dateHourExtractMap ---- Map<"日期"，Map<"小时段"，List<Integer>(抽取数据索引)>>
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        Random random = new Random();
        //dateHourCountMap<String,Map<String,Long>>
        //<2018-08-09,Map<11,400>>
        for (Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
            String date = entry.getKey();
            /**
             * hourCountMap  key:hour  value:carCount
             * 当前日期下，每小时对应的车辆数
             * Map<11,400>>
             *     <12,500>
             */
            Map<String, Long> hourCountMap = entry.getValue();

            //计算出这一天总的车流量
            long dateCarCount = 0L;

            for (long tmpHourCount : hourCountMap.values()) {
                dateCarCount += tmpHourCount;
            }

            /**
             * 小时段对应的应该抽取车辆的索引集合
             * hourExtractMap ---- Map<小时，List<>>
             */
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            /**
             * 遍历的是每个小时对应的车流量总数信息
             * hourCountMap  key:hour  value:carCount
             */
            for (Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                //当前小时段
                String hour = hourCountEntry.getKey();
                //当前小时段对应的真实的车辆数
                long hourCarCount = hourCountEntry.getValue();

                //计算出这个小时的车流量占总车流量的百分比,然后计算出在这个时间段内应该抽取出来的车辆信息的数量

                int hourExtractNum = (int) (((double) hourCarCount / (double) dateCarCount) * extractNumPerDay);

                /**
                 * 如果在这个时间段内抽取的车辆信息数量比这个时间段内的车流量还要多的话，只需要将count的值赋值给hourExtractNum就可以
                 * 代码健壮性
                 */
                if (hourExtractNum >= hourCarCount) {
                    hourExtractNum = (int) hourCarCount;
                }

                //获取当前小时 存储随机数的List集合
                List<Integer> extractIndexs = hourExtractMap.get(hour);
                if (extractIndexs == null) {
                    extractIndexs = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexs);
                }

                /**
                 * 生成抽取的car的index，  实际上就是生成一系列的随机数   随机数的范围就是0-count(这个时间段内的车流量) 将这些随机数放入一个list集合中
                 * 那么这里这个随机数的最大值没有超过实际上这个时间点对应的中的车流量总数，这里的list长度也就是要抽取数据个数的大小。
                 * 假设在一天中，7~8点这个时间段总车流量为100，假设我们之前刚刚算出应该在7~8点抽出的车辆数为20
                 * 那么 我们怎么样随机抽取呢？
                 * 1.循环20次
                 * 2.每次循环搞一个0~100的随机数，放入一个list<Integer>中，那么这个list中的每一个元素就是我们这里说的car的index
                 * 3.最后得到一个长度为20的car的indexList<Integer>集合，一会取值，取20个，那么取哪个值呢，就取这里List中的下标对应的car
                 *
                 */
                for (int i = 0; i < hourExtractNum; i++) {

                    int index = random.nextInt((int) hourCarCount);
                    while (extractIndexs.contains(index)) {
                        index = random.nextInt((int) hourCarCount);
                    }
                    extractIndexs.add(index);
                }


                System.out.println("----");
            }
        }

        /******************************************************************/
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new HashMap<String, Map<String, IntList>>();

        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();

                IntList fastutilExtractList = new IntArrayList();


                for (int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

//			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
//		}
            /******************************************************************/

            JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

            final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractBroadcast = sc.broadcast(dateHourExtractMap);
            /**
             * 在dateHour2DetailRDD中进行随机抽取车辆信息，
             * 首先第一步：按照date_hour进行分组，然后对组内的信息按照 dateHourExtractBroadcast参数抽取相应的车辆信息
             * 抽取出来的结果直接放入到MySQL数据库中。
             *
             * extractCarRDD ----抽取出来的所有车辆
             */
            JavaPairRDD<String, String> extractCarRDD = dateHour2DetailRDD.groupByKey()
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

                        /**
                         *
                         */
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {

                            //将要返回的当前日期当前小时段下抽取出来的车辆集合
                            List<Tuple2<String, String>> list = new ArrayList<>();
                            //按index下标抽取的这个时间段对应的车辆集合
                            List<RandomExtractCar> carRandomExtracts = new ArrayList<>();

                            //2017-10-10_08
                            String dateHour = t._1;
                            //Iterator<car>=>Iterator<car = "xxx">
                            Iterator<String> iterator = t._2.iterator();

                            String date = dateHour.split("_")[0];
                            String hour = dateHour.split("_")[1];
                            // <2018-08-08,<10,List<200,32,77>>
                            Map<String, Map<String, List<Integer>>> dateHourExtractMap = dateHourExtractBroadcast.value();

                            List<Integer> indexList = dateHourExtractMap.get(date).get(hour);

                            int index = 0; // index
                            while (iterator.hasNext()) {
                                //car="沪A001"
                                String value = iterator.next();

                                String car = StringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_CAR);

                                if (indexList.contains(index)) {

                                    RandomExtractCar carRandomExtract = new RandomExtractCar(taskId, car, date, dateHour);
                                    carRandomExtracts.add(carRandomExtract);

                                    list.add(new Tuple2<>(car, car));
                                }
                                index++;
                            }
                            /**
                             * 将抽取出来的车辆信息插入到random_extract_car表中
                             */
                            IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                            randomExtractDAO.insertBatchRandomExtractCar(carRandomExtracts);


                            return list.iterator();

                        }
                    });

            /**
             * extractCarRDD  K:car V:car
             * 抽取到的所有的car，这里去和开始得到的符合日期内的车辆详细信息car2DetailRDD ，得到抽取到的car的详细信息
             *
             */


            JavaPairRDD<String, Row> randomExtractCar2DetailRDD =
                    extractCarRDD.join(car2DetailRDD).mapPartitionsToPair(new PairFlatMapFunction<
                            Iterator<Tuple2<String, Tuple2<String, Row>>>, String, Row>() {

                        /**
                         *
                         */
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Iterator<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                            List<RandomExtractMonitorDetail> randomExtractMonitorDetails = new ArrayList<>();
                            IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                            List<Tuple2<String, Row>> list = new ArrayList<>();

                            while (iterator.hasNext()) {
                                Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                                Row row = tuple._2._2;
                                String car = tuple._1;

                                RandomExtractMonitorDetail m = new RandomExtractMonitorDetail(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                                randomExtractMonitorDetails.add(m);
                                list.add(new Tuple2<>(car, row));
                            }
                            //将车辆详细信息插入random_extract_car_detail_info表中。
                            randomExtractDAO.insertBatchRandomExtractDetails(randomExtractMonitorDetails);

                            return list.iterator();
                        }
                    });

            return randomExtractCar2DetailRDD;
        }
        return null;
    }

}

