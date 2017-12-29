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
public class PlayersValidator extends  IValid{
    @Override
    public DataFrame Validate(DataFrame df)
    {
        DataFrame errorRows=df.filter((col("fromTeamName").isNull())).select("rowNumber");
        printErrors(errorRows,"From player not found in database.");
        errorRows=df.filter((col("toTeamName").isNull()).and(df.col("to").isNotNull())).select("rowNumber");
        printErrors(errorRows,"To player not found in database.");
        return df.filter(col("fromTeamName").isNotNull().and((df.col("to").isNull()).or((df.col("to").isNotNull()).and((col("toTeamName").isNotNull())))));
    }
}

