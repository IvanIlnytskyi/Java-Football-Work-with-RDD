package football.EntitiesForDFBuilding;

import java.io.Serializable;
import java.util.HashMap;

public class Event implements Serializable {
    public HashMap<String,String> attributes;
    public Event(){attributes=new HashMap<>();}
    public void AddAttribute(String key,String value)
    {
        attributes.put(key,value);
    }
    public String GetAttribute(String key)
    {
        return attributes.get(key);
    }
}
