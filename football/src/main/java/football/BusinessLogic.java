package football;

import football.Validators.CodeValidator;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Console;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class BusinessLogic {
    @Autowired
    private DataFramesGeneralizer generalizer;

    @Autowired
    private DataFrameValidator dataFrameValidator;

    public void doWork()
    {
        DataFrame rawInfo=generalizer.loadGeneralizedDataFrame();
        System.out.println("Raw events!!!");
        rawInfo.show();
        DataFrame validatedInfo =dataFrameValidator.Validate(rawInfo);
        System.out.println("Validated events!!!");
        validatedInfo.show();

    }
}
