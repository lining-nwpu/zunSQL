package npu.zunsql.treemng;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

//import com.sun.org.apache.bcel.internal.generic.I2F;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class Table implements TableReader, Serializable {
	protected String tableName;

	protected Column keyColumn;

	protected List<Column> columns;

	protected LockType lock;

	private int rootNodePage;//表示本表的根节点页号很危险不要使用
	
	private int pageID = -1;//表示本表的根节点页号很危险不要使用

	public CacheMgr cacheManager;

	private Page pageOne;//表示本表的根节点页号的页很危险不要使用

	private boolean writeMyPage(Transaction myTran) throws IOException 
	{
		return cacheManager.writePage(myTran.tranNum, pageOne);
	}

	private void intoBytes(Transaction thisTran) throws IOException {
		byte[] bytes = new byte[Page.PAGE_SIZE];
		ByteArrayOutputStream byt = new ByteArrayOutputStream();

		ObjectOutputStream obj = new ObjectOutputStream(byt);
		obj.writeObject(tableName);
		obj.writeObject(keyColumn.getName());
		List<String>list1=new ArrayList<String>();
		List<String>list2=new ArrayList<String>();
		for(Column c1:columns)
		{
			list1.add(c1.getName());
			list2.add(c1.getType().toString());
		}
		obj.writeObject(list1);
	    obj.writeObject(list2);
	    
	    List<Integer>list3=new ArrayList<Integer>();
	    obj.writeObject(list3);
	    Integer i=-1;
	    obj.writeObject(i);
	    Integer ii=this.rootNodePage;
		obj.writeObject(ii);
		
		Integer M=12;
		
		obj.writeObject(M);
		
		List<Row>list4=new ArrayList<Row>();
		
		obj.writeObject(list4);
		
		bytes = byt.toByteArray();
		pageOne.getPageBuffer().rewind();
		pageOne.getPageBuffer().put(bytes);
		cacheManager.writePage(thisTran.tranNum, pageOne);
		// thisTran.Commit();
	}

	public String get_keyColumn()
	{
		return this.keyColumn.getName();
	}

	protected Table(int pageID, CacheMgr cacheManager, Transaction thisTran)
			throws IOException, ClassNotFoundException 
	{
		super();
		this.cacheManager = cacheManager;
		pageOne = this.cacheManager.readPage(thisTran.tranNum, pageID);

		ByteBuffer thisBufer = pageOne.getPageBuffer();
		thisBufer.rewind();
		byte[] bytes = new byte[Page.PAGE_SIZE];
		thisBufer.get(bytes, 0, thisBufer.remaining());

		ByteArrayInputStream byteTable = new ByteArrayInputStream(bytes);
		ObjectInputStream objTable = new ObjectInputStream(byteTable);

		this.tableName = (String) objTable.readObject();
		String s1=(String)objTable.readObject();
		
		List<String>list1=new ArrayList<String>();
		List<String>list2=new ArrayList<String>();
		list1=(List<String>)objTable.readObject();
		list2=(List<String>)objTable.readObject();
		
		this.columns=new ArrayList<Column>();
		
		for(int i=0;i<list1.size();i++)
		{
			if(list1.get(i).contentEquals(s1))
			{
				this.keyColumn=new Column(BasicType.valueOf(list2.get(i)),list1.get(i),i);
			}
			Column c1=new Column(BasicType.valueOf(list2.get(i)),list1.get(i),i);
			this.columns.add(c1);
		}
		
		objTable.readObject();
		objTable.readObject();
		this.rootNodePage = pageID;
		this.pageID=this.rootNodePage;
	}

	protected Integer getTablePageID() {
		return pageOne.getPageID();
	}

	protected Column getKeyColumn() {
		return keyColumn;
	}

	protected Node getRootNode(Transaction thisTran) throws IOException, ClassNotFoundException 
	{
		int i;
		int search_position=-1000;
		Node n1=new Node(-2,cacheManager,thisTran);
    	Cell c1=new Cell(tableName);
    	while(true)
    	{
    		for(i=0;i<n1.rowList.size();i++)
    		{
    			if(n1.rowList.get(i).getCell(0).bigerThan(c1))
    			{
    				break;
    			}
    		}
    		
    		if(i>0&&n1.rowList.get(i-1).getCell(0).equalTo(c1))
    		{
    			search_position=i-1;
    			break;
    		}
    		else {
    			if(i<n1.sonNodeList.size())
    			{
    				n1=new Node(n1.sonNodeList.get(i),cacheManager,thisTran);
    			}
    			else {
    				break;
    			}
    		}
    	}
    	//查master表
    	if(search_position!=-1000)
		{
    		return new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()), cacheManager, thisTran);
		}
    	else {
    		return new Node(0,cacheManager,thisTran);
    	}
		
	}

	protected Column getColumn(String columnName) 
	{
		for (int i = 0; i < columns.size(); i++) 
		{
			if (columns.get(i).getName().equals(columnName)) 
			{
				return columns.get(i);
			}
		}
		return null;
	}

	protected void writeRootNodePage(int id, Transaction thisTran) throws IOException 
	{
		rootNodePage = id;
		intoBytes(thisTran);

	}

	public Cursor createCursor(Transaction thistran) throws IOException, ClassNotFoundException 
	{
		return new TableCursor(this, thistran);
	}

	public List<String> getColumnsName() {
		List<String> sList = new ArrayList<String>();
		for (int i = 0; i < columns.size(); i++) 
		{
			sList.add(columns.get(i).getName());
		}
		return sList;
	}

	public List<BasicType> getColumnsType() {
		List<BasicType> sList = new ArrayList<BasicType>();
		for (int i = 0; i < columns.size(); i++) 
		{
			sList.add(columns.get(i).getType());
		}
		return sList;
	}

	public String getTableName() {
		return tableName;
	}

	public boolean isLocked() {
		if (lock == LockType.Locked) 
		{
			return true;
		}
		else {
			return false;
		}
	}

	public boolean lock(Transaction thistran) throws IOException {
		lock = LockType.Locked; 

		intoBytes(thistran);

		while (!writeMyPage(thistran))
			;
		return true;
	}

	public boolean unLock(Transaction thistran) throws IOException {
		lock = LockType.Shared;

		intoBytes(thistran);
		while (!writeMyPage(thistran))
			;
		return true;
	}
}
