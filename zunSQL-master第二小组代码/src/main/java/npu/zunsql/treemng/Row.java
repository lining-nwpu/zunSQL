package npu.zunsql.treemng;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/30.
 */
public class Row implements Serializable
{
	//public List<String> s1=new ArrayList<String>();因为rowlist太大暂时去掉
	
    public List<Cell> cellList = new ArrayList<Cell>();

    public Row(List<String> SList)
    {
        for(int i = 0; i < SList.size(); i++)
        {
        	//s1.add(SList.get(i));
            cellList.add(new Cell(SList.get(i)));
        }
    }

    public List<String> getStringList()
    {
        List<String> SList = new ArrayList<String>();
        for(int i = 0; i < cellList.size(); i++)
        {
            SList.add(cellList.get(i).getValue_s());
        }
        return SList;
    }

    public Cell getCell(int array)
    {
        return cellList.get(array);
    }
}
