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
public class TeamValidator extends  IValid{
    @Override
    public DataFrame Validate(DataFrame df)
    {
        DataFrame errorRows=df.filter(col("isCodeBinary").equalTo(true).and(col("fromTeamName").notEqual(col("toTeamName")))).select("rowNumber");
        printErrors(errorRows,"Players should be in the same team.");
        return df.filter(col("isCodeBinary").equalTo(false).or(col("isCodeBinary").equalTo(true).and(col("fromTeamName").equalTo(col("toTeamName")))));
    }

}

