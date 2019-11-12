package npu.zunsql.cache;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Page implements Serializable
{
	
	private static final long serialVersionUID = 1L;
	public static final int PAGE_SIZE = 1024;
    
	public static int pageCount;
    protected static List<Integer> unusedID =  new ArrayList<Integer>();
    protected int pageID;
    protected ByteBuffer pageBuffer = null;
    
    public static CacheMgr c;

    public Page(ByteBuffer buffer)
    {
        //TODO:unusedLIst
    	
    	
    	if(c.unusedList_PageID.size()==0)
    	{
    		this.pageID = pageCount;
    		pageCount++;
    	}
    	
    	else {
    		this.pageID=c.unusedList_PageID.get(0);
    		c.unusedList_PageID.remove(0);
    	}
    	
    	//原先的在下面
        /*if(Page.unusedID.isEmpty())
        {
            this.pageID = pageCount++;
        }
        else{
            this.pageID = Page.unusedID.indexOf(0);
            Page.unusedID.remove(0);
        }*/
    	//原先的在上面
    	
        this.pageBuffer = buffer;
    }

    public Page(int pageID, ByteBuffer buffer)
    {
        this.pageID = pageID;
        this.pageBuffer = buffer;
    }

    public Page(Page page)
    {
        this.pageID = page.pageID;
        ByteBuffer tempBuffer = ByteBuffer.allocate(page.pageBuffer.capacity());
        tempBuffer.put(page.pageBuffer);
        this.pageBuffer = tempBuffer;
    }

    public int getPageID()
    {
        return this.pageID;
    }

    public ByteBuffer getPageBuffer()
    {
        return this.pageBuffer;
    }
}