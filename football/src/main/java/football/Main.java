package football;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import scala.Console;
import org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Conf.class);
        BusinessLogic businessLogic=context.getBean(BusinessLogic.class);
        businessLogic.doWork();
    }
}
