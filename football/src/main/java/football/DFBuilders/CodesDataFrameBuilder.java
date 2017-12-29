package football.DFBuilders;


import football.EntitiesForDFBuilding.CodeInfo;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

@Service
public class CodesDataFrameBuilder {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private SQLContext sqlContext;

    public DataFrame load() {
        try {
            File file = new File(getClass().getClassLoader().getResource("codes.properties").getFile());
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInput);
            fileInput.close();
            Enumeration enuKeys = properties.keys();
            List codesInfo= new ArrayList();

            while (enuKeys.hasMoreElements()) {
                String key = (String) enuKeys.nextElement();
                String[] values = properties.getProperty(key).split(",");
                codesInfo.add(new CodeInfo(Integer.parseInt(key), values[0],Integer.parseInt(values[1])==1));
            }
            JavaRDD<CodeInfo> rdd = sc.parallelize(codesInfo);
            JavaRDD<Row> rowJavaRDD = rdd.map(line -> {
                return RowFactory.create(line.Id,line.Description,line.IsBinary);
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
                DataTypes.createStructField("codeId", DataTypes.IntegerType, false),
                DataTypes.createStructField("codeDescription", DataTypes.StringType, false),
                DataTypes.createStructField("isCodeBinary", DataTypes.BooleanType, false)
        });
    }

}
