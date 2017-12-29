package football.EntitiesForDFBuilding;

import java.io.Serializable;

public class PlayerInfo implements Serializable {
    public String NameSurname;
    public String Team;
    public PlayerInfo(String n,String t)
    {
        NameSurname=n;
        Team=t;
    }
}
