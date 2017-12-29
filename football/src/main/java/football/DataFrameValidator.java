package football;

import football.Validators.*;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.crypto.Data;

@Service
public class DataFrameValidator {
    @Autowired
    CodeValidator codeValidator;

    @Autowired
    NumberOfPlayersValidator numberOfPlayersValidator;

    @Autowired
    PlayersValidator playersValidator;

    @Autowired
    TeamValidator teamValidator;

    @Autowired
    TimeValidator timeValidator;

    public DataFrame Validate(DataFrame df)
    {
        return timeValidator.Validate(teamValidator.Validate(playersValidator.Validate(numberOfPlayersValidator.Validate(codeValidator.Validate(df)))));
    }
}
