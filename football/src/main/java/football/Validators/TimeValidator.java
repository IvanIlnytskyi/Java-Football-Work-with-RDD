package football.Validators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;

import static org.apache.spark.sql.functions.*;

import java.awt.*;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;
import scala.Console;
@Service
public class TimeValidator extends  IValid{
    @Override
    public DataFrame Validate(DataFrame df)
    {
        DataFrame errorRows=df.filter(not(col("eventTime").rlike("^(90|[1-8][0-9]|[0-9]):([0-9]|[1-5][0-9]|60)$")));
        printErrors(errorRows,"Invalid event time.");
        return df.filter(col("eventTime").rlike("^(90|[1-8][0-9]|[0-9]):([0-9]|[1-5][0-9]|60)$"));
    }

}

