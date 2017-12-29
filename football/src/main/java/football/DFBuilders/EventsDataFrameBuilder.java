package football.DFBuilders;


import football.EntitiesForDFBuilding.Event;
import lombok.ToString;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class EventsDataFrameBuilder {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private SQLContext sqlContext;


    public DataFrame load() {
        try{

            File file = new File("data/rawData.txt");
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            List events = new ArrayList();
            int increment=1;
            while ((line = br.readLine()) != null) {
                Event event=new Event();
                for(String attribute:line.split(";"))
                {
                    String[] pair = attribute.split("=");
                    event.AddAttribute(pair[0],pair[1]);
                }
                event.AddAttribute("rowNumber", Integer.toString(increment++));
                events.add(event);
            }

            JavaRDD<Event> rdd = sc.parallelize(events);
            JavaRDD<Row> rowJavaRDD = rdd.map(event -> {
                return RowFactory.create(   Integer.parseInt(event.GetAttribute("rowNumber")),
                                            Integer.parseInt(event.GetAttribute("code")),
                                            event.GetAttribute("from"),
                                            event.GetAttribute("to"),
                                            event.GetAttribute("eventTime"),
                                            event.GetAttribute("stadion")
                                        );
            });
            return sqlContext.createDataFrame(rowJavaRDD,createSchema());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sqlContext.createDataFrame(sc.emptyRDD(),createSchema());

    }


    private static StructType createSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("rowNumber", DataTypes.IntegerType, false),
                DataTypes.createStructField("codeId", DataTypes.IntegerType, false),
                DataTypes.createStructField("from", DataTypes.StringType, false),
                DataTypes.createStructField("to", DataTypes.StringType, true),
                DataTypes.createStructField("eventTime", DataTypes.StringType,false),
                DataTypes.createStructField("stadion", DataTypes.StringType, false)
        });
    }

}

