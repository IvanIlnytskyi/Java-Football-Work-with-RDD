package football.DFBuilders;

import football.EntitiesForDFBuilding.PlayerInfo;
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
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

@Service
public class TeamDataFrameBuilder {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private SQLContext sqlContext;

    public DataFrame load() {
        try {

         //   Properties prop = new Properties();
       //     ClassLoader loader = Thread.currentThread().getContextClassLoader();
     //       InputStream stream = loader.getResourceAsStream("myProp.properties");
   //         prop.load(stream);

            File file = new File(getClass().getClassLoader().getResource("teams.properties").getFile());
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInput);
            fileInput.close();
            Enumeration enuKeys = properties.keys();
            List playerInfo= new ArrayList();
            while (enuKeys.hasMoreElements()) {
                String key = (String) enuKeys.nextElement();
                String[] values = properties.getProperty(key).split(",");
                for (String name : values) {
                    playerInfo.add(new PlayerInfo(name, key));
                }
            }
            JavaRDD<PlayerInfo> rdd = sc.parallelize(playerInfo);
            JavaRDD<Row> rowJavaRDD = rdd.map(line -> {
                return RowFactory.create(line.NameSurname,line.Team);
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
                DataTypes.createStructField("playerName", DataTypes.StringType, false),
                DataTypes.createStructField("teamName", DataTypes.StringType, false)
        });
    }

}
