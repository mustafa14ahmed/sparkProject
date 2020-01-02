package com.org.sparkApp.Spark;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import java.util.Date;

import java.util.logging.*;
import java.util.List;


public class UseCase_2 {

    static Dataset<Row> df_load;
    static Dataset<Row> df_set;
    static Dataset<Row> temp_df;
    static Dataset<Row> collect_df;
    static Dataset<Row> final_df;
    static SparkSession spark;
    static SparkConf conf;
    static JavaSparkContext sc;
    static List<Row> table_list;
    static String tablename;
    static String database;
    static String cluster;;

    static String Load_File = "C:\\Users\\Mustafa-PC\\Desktop\\Data files\\data-load-files\\data-load-files\\sample_table.csv";
    static String Set_File = "C:\\Users\\Mustafa-PC\\Desktop\\Data files\\data-load-files\\data-set-files\\Data-set\\";
    static File Out_path = new File("C:\\Users\\Mustafa-PC\\Desktop\\Data files\\data-load-files\\Mustafa\\Output\\");
    static String final_output = "C:\\Users\\Mustafa-PC\\Desktop\\Data files\\data-load-files\\Mustafa\\Final_Output\\";
    static String log_info = "C:\\Users\\Mustafa-PC\\Desktop\\Data files\\data-load-files\\Mustafa\\Log\\";
    static Logger LOGGER = Logger.getLogger("MyLog");
    static FileHandler fh;

    public static void main(String[] args) throws IOException {

        try {
            conf = new SparkConf().setAppName("UseCase2").set("spark.ui.port", "12346").setMaster("local[*]");
            spark = SparkSession.builder().config(conf).getOrCreate();
            sc = new JavaSparkContext(spark.sparkContext());

            FileUtils.deleteDirectory(Out_path);

            String pattern = "yyyy-MM-dd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

            String date = simpleDateFormat.format(new Date());

            fh = new FileHandler(log_info + date + ".log");
            LOGGER.addHandler(fh);

            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            LOGGER.info("Program Started");

            File file = new File(Set_File);
            File[] files = file.listFiles(new FilenameFilter() {


                public boolean accept(File dir, String name) {
                    if (name.toLowerCase().endsWith(".csv")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            });


            df_load = spark.read().format("csv").option("header", "true").load(Load_File).select("table_name",
                    "database", "cluster");

            table_list = df_load.collectAsList();

            for (Row s : table_list) {
                for (File f : files) {
                    if (f.getName().substring(0, f.getName().lastIndexOf(".")).equalsIgnoreCase(s.get(0).toString())) {
                        // String table_name = s.get(0).toString();
                        df_set = spark.read().format("csv").option("header", "true")
                                .load(Set_File + f.getName().toString());

                        tablename = s.get(0).toString();
                        database = s.get(1).toString();
                        cluster = s.get(2).toString();

                        df_set.createOrReplaceTempView("Temp_table");

                        for (String schema : df_set.columns()) {

                            min_stats(schema);
                            max_stats(schema);
                            avg_stats(schema);
                            distinct_stats(schema);
                            null_stats(schema);

                        }

                    }

                }
            }

            collect_df = spark.read().format("csv").option("header", "true").load(Out_path.toString())
                    .orderBy("table_name", "column_name");

            collect_df.coalesce(1).write().format("csv").option("header", "true").mode(SaveMode.Overwrite)
                    .save(final_output);

        } catch (Exception e) {

            LOGGER.info("Something went wrong" + e.toString());

        } finally {
            FileUtils.deleteDirectory(Out_path);
            sc.stop();
            sc.close();
            spark.stop();
            spark.close();
            LOGGER.info("Session Closed for SparkContext and SparkSession");
        }

    }

    static public void min_stats(String schema) {
        temp_df = spark.sql("select "  + "'" + cluster + "'" + "as cluster,"
                + "'" + database + "'" + " as db,'test_schema' as schema_name," + "'" + tablename + "'"
                + "as table_name,"+ "'" + schema + "'" + " as column_name ,  'min' as stats_name," + "min(" + schema + ") as stats_value,"
                + "UNIX_TIMESTAMP(current_timestamp) as start_epoch,UNIX_TIMESTAMP(current_timestamp) as end_epoch "
                + "from Temp_table group by column_name");

        temp_df.coalesce(1).write().format("csv").option("header", "true").mode(SaveMode.Append)
                .save(Out_path.toString());

    }

    static public void max_stats(String schema) {
        temp_df = spark.sql("select "  + "'" + cluster + "'" + "as cluster,"
                + "'" + database + "'" + " as db,'test_schema' as schema_name," + "'" + tablename + "'"
                + "as table_name,"+ "'" + schema + "'" + " as column_name ,  'max' as stats_name," + "max(" + schema + ") as stats_value,"
                + "UNIX_TIMESTAMP(current_timestamp) as start_epoch,UNIX_TIMESTAMP(current_timestamp) as end_epoch "
                + "from Temp_table group by column_name");

        temp_df.coalesce(1).write().format("csv").option("header", "true").mode(SaveMode.Append)
                .save(Out_path.toString());

    }

    static public void avg_stats(String schema) {
        temp_df = spark.sql("select "  + "'" + cluster + "'" + "as cluster,"
                + "'" + database + "'" + " as db,'test_schema' as schema_name," + "'" + tablename + "'"
                + "as table_name,"+ "'" + schema + "'" + " as column_name ,  'avg' as stats_name," + "avg(" + schema + ") as stats_value,"
                + "UNIX_TIMESTAMP(current_timestamp) as start_epoch,UNIX_TIMESTAMP(current_timestamp) as end_epoch "
                + "from Temp_table group by column_name");

        temp_df.coalesce(1).write().format("csv").option("header", "true").mode(SaveMode.Append)
                .save(Out_path.toString());
    }

    static public void distinct_stats(String schema) {
        temp_df = spark.sql("select "  + "'" + cluster + "'" + "as cluster,"
                + "'" + database + "'" + " as db,'test_schema' as schema_name," + "'" + tablename + "'"
                + "as table_name,"+ "'" + schema + "'" + " as column_name ,  'distinct' as stats_name," + "count(distinct(" + schema + ")) as stats_value,"
                + "UNIX_TIMESTAMP(current_timestamp) as start_epoch,UNIX_TIMESTAMP(current_timestamp) as end_epoch "
                + "from Temp_table group by column_name");

        temp_df.coalesce(1).write().format("csv").option("header", "true").mode(SaveMode.Append)
                .save(Out_path.toString());
    }

    static public void null_stats(String schema) {
        temp_df = spark.sql("select "  + "'" + cluster + "'" + "as cluster,"
                + "'" + database + "'" + " as db,'test_schema' as schema_name," + "'" + tablename + "'"
                + "as table_name,"+ "'" + schema + "'" + " as column_name ,  'NULL' as stats_name," + "SUM(CASE WHEN " + schema
                + " is NULL THEN 1 ELSE 0 END) as stats_value,"
                + "UNIX_TIMESTAMP(current_timestamp) as start_epoch,UNIX_TIMESTAMP(current_timestamp) as end_epoch "
                + "from Temp_table group by column_name");

        temp_df.coalesce(1).write().format("csv").option("header", "true").mode(SaveMode.Append)
                .save(Out_path.toString());
    }

}


