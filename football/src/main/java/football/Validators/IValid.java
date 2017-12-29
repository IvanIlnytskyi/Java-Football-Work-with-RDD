package football.Validators;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.awt.*;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.col;


public class IValid {
    DataFrame Validate (DataFrame df){return df;}
    void printErrors(DataFrame err,String message)
    {
        if(err.count()!=0) {
            try {
                FileWriter fstream = new FileWriter("data/ErrorLogs.txt", true);
                Row[] rows = err.collect();
                String s = "";
                for (Row row : rows) {
                    s += row.get(0).toString() + "; ";
                }
                fstream.append(String.format("%1$s Error in Rows - %2$s\n", message, s));
                fstream.close();
            } catch (IOException e) {
            }
        }
    }

}
