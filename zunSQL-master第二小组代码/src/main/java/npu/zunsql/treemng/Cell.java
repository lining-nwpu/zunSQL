package npu.zunsql.treemng;

import java.io.Serializable;

/**
 * Created by Ed on 2017/10/30.
 */
public class Cell implements Serializable
{
    public String sValue;

    public Cell(String  givenValue)
    {
        sValue = givenValue;
    }

    public Cell(Integer  givenValue)
    {
        sValue = givenValue.toString();
    }

    public Cell(Double  givenValue)
    {
        sValue = givenValue.toString();
    }

    public boolean bigerThan(Cell cell1)
    {
    	if(sValue.matches("-?[0-9]+.?[0-9]*")&&cell1.sValue.matches("-?[0-9]+.?[0-9]*"))
    	{
    		double d1,d2;
    		d1=Double.parseDouble(sValue);
    		d2=Double.parseDouble(cell1.sValue);
    		if(d1>d2)
    		{
    			return true;
    		}
    		else {
    			return false;
    		}
    	}
        return sValue.compareTo(cell1.getValue_s()) > 0;
    }

    public boolean letterThan(Cell cell1)
    {
    	if(sValue.matches("-?[0-9]+.?[0-9]*")&&cell1.sValue.matches("-?[0-9]+.?[0-9]*"))
    	{
    		double d1,d2;
    		d1=Double.parseDouble(sValue);
    		d2=Double.parseDouble(cell1.sValue);
    		if(d1<d2)
    		{
    			return true;
    		}
    		else {
    			return false;
    		}
    	}
        return sValue.compareTo(cell1.getValue_s()) < 0;
    }
    
    public boolean equalTo(Cell cell)
    {
         return sValue.contentEquals(cell.getValue_s());

    }

    public String getValue_s()
    {
        return sValue;
    }
    
    public Integer getValue_i()
    {
        return Integer.valueOf(sValue);
    }
    
    public Double getValue_d()
    {
        return Double.valueOf(sValue);
    }
    
    public void setCell(String s1)
    {
    	this.sValue=s1;
    }
}
