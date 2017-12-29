package football;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
public class Conf {
    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(new SparkConf().setMaster("local").setAppName("football"));
    }

    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc());
    }

}
