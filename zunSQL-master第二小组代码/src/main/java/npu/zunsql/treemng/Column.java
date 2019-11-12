package npu.zunsql.treemng;

import javafx.scene.control.Tab;

import java.io.Serializable;
import java.util.List;

import static java.lang.Math.abs;

/**
 * Created by Ed on 2017/10/29.
 */
public class Column implements Serializable
{
    private BasicType columnType;

    private String columnName;

    private int columnNumber;

    protected Column(BasicType type, String name, Integer number)
    {
        columnType = type;
        columnName = name;
        columnNumber = number;
    }

    protected BasicType getType()
    {
        return columnType;
    }

    protected String getName()
    {
        return columnName;
    }

    protected Integer getNumber()
    {
        return columnNumber;
    }
}
