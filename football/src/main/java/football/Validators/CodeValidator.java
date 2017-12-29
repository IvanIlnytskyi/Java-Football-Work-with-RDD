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
public class CodeValidator extends  IValid{
    @Override
    public DataFrame Validate(DataFrame df)
    {
            DataFrame errorRows=df.filter(col("codeDescription").isNull()).select("rowNumber");
            printErrors(errorRows,"No description for codes found.");
            return df.filter(col("codeDescription").isNotNull());
    }

}
