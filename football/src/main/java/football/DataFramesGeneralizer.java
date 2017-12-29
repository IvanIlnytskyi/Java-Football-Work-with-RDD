package football;

//import com.google.common.collect.RegularImmutableTable;
import football.DFBuilders.CodesDataFrameBuilder;
import football.DFBuilders.EventsDataFrameBuilder;
import football.DFBuilders.TeamDataFrameBuilder;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

@Service
public class DataFramesGeneralizer {
    @Autowired
    private EventsDataFrameBuilder eventsDataFrameBuilder;

    @Autowired
    private TeamDataFrameBuilder teamDataFrameBuilder;

    @Autowired
    private CodesDataFrameBuilder codesDataFrameBuilder;

    private DataFrame loadEvents()
    {
        return eventsDataFrameBuilder.load();
    }

    private DataFrame loadTeams()
    {
        return  teamDataFrameBuilder.load();
    }

    private  DataFrame loadCodes()
    {
        return codesDataFrameBuilder.load();
    }
    public DataFrame loadGeneralizedDataFrame()
    {
        DataFrame events = loadEvents();
        DataFrame codes=loadCodes();
        DataFrame from = loadTeams();
        DataFrame to =loadTeams();
        from=from.withColumnRenamed("playerName","fromPlayerName");
        from=from.withColumnRenamed("teamName","fromTeamName");
        to=to.withColumnRenamed("playerName","toPlayerName");
        to=to.withColumnRenamed("teamName","toTeamName");
        return events.join(codes,events.col("codeId").equalTo(codes.col("codeId")),"left_outer")
                .join(from,events.col("from").equalTo(from.col("fromPlayerName")),"left_outer")
                .join(to,events.col("to").equalTo(to.col("toPlayerName")),"left_outer")
                .orderBy(events.col("rowNumber"));


    }
}
