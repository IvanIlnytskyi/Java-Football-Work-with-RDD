package football.EntitiesForDFBuilding;

import java.io.Serializable;

public class CodeInfo implements Serializable{
    public int Id;
    public String Description;
    public boolean IsBinary;
    public CodeInfo(int id,String d,boolean i)
    {
        this.Id=id;
        this.Description=d;
        this.IsBinary=i;
    }
}
