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
public class NumberOfPlayersValidator extends  IValid{
    @Override
    public DataFrame Validate(DataFrame df)
    {
        DataFrame errorRows=df.filter((df.col("from").isNull())).select("rowNumber");
        printErrors(errorRows,"Player who made event whould be declared.");
        errorRows=df.filter(col("isCodeBinary").equalTo(false).and(df.col("to").isNotNull())).select("rowNumber");
        printErrors(errorRows,"Too many players as arguments.");
        errorRows=df.filter(col("isCodeBinary").equalTo(true).and(df.col("to").isNull())).select("rowNumber");
        printErrors(errorRows,"Too few players as arguments.");
        return df.filter((df.col("from").isNotNull()).and(
                                                            (col("isCodeBinary").equalTo(false).and(df.col("to").isNull()))
                                                            .or(col("isCodeBinary").equalTo(true).and(df.col("to").isNotNull())))
                                                        );
    }

}
