package npu.zunsql.treemng;

import npu.zunsql.cache.Page;
import npu.zunsql.cache.CacheMgr;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/29.
 */
public class Database {

	public String dBName;

	public Integer M;
	
	public CacheMgr cacheManager;

	public String getDatabaseName() {
		return dBName;
	}

	public Database(String name, int M) throws IOException, ClassNotFoundException 
	{
		dBName = name;
		cacheManager = new CacheMgr(dBName);
		
		boolean dbisNew = false;
		dbisNew = cacheManager.isNew();
		
		Page.c=cacheManager;
		
		if (!dbisNew) 
		{
			Transaction initTran = beginReadTrans();
			
			Node n1=new Node(-2,cacheManager);
			
			this.M=n1.M;
			
			//master = new Table(0, cacheManager, initTran);去掉
			initTran.Commit();
		} 
		else {
			Transaction initTran = beginWriteTrans();
			addMaster(initTran, M);
			initTran.Commit();
		}
	}

	public Database(String name) throws IOException, ClassNotFoundException 
	{
		this(name, 5);
	}

	private boolean addMaster(Transaction initTran, int M) throws IOException, ClassNotFoundException 
	{
		List<String> sList = new ArrayList<String>();
		List<BasicType> tList = new ArrayList<BasicType>();
		sList.add("tableName");
		sList.add("pageNumber");
		sList.add("indexpage");//-1表示没有索引页
		tList.add(BasicType.String);
		tList.add(BasicType.Integer);
		tList.add(BasicType.Integer);
		this.M=M;
		createTable("master", "tableName", sList, tList, initTran, M);
		
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);
        
        Integer random;
        random=Page.pageCount;
        obj.writeObject(random);
        Integer random2;
        random2=0; //最一开始master表在第0页
        obj.writeObject(random2);
        
        Page ppp=this.cacheManager.readPage(initTran.tranNum, -2);
        ppp.getPageBuffer().rewind();
        ppp.getPageBuffer().put(byt.toByteArray());
        cacheManager.writePage(initTran.tranNum,ppp);

		List<String> masterRow_s = new ArrayList<String>();
		masterRow_s.add("master");
		masterRow_s.add("0");
		masterRow_s.add("-1");

		Row r1=new Row(masterRow_s);
		
		Node master=new Node(-2,cacheManager,initTran);
		master.insertRow(r1, initTran);
		
		/*Cursor masterCursor = master.createCursor(initTran);
		masterCursor.insert(initTran, masterRow_s);去掉*/

		return true;
	}

	public boolean close() throws ClassNotFoundException 
	{
		cacheManager.close();
		return true;
	}

	public Transaction beginReadTrans() {
		return new ReadTran(cacheManager.beginTransation("r"), cacheManager);
	}

	public Transaction beginWriteTrans() 
	{
		return new WriteTran(cacheManager.beginTransation("w"), cacheManager);
	}
	
	public Transaction beginUserTrans() {
//		return new UserTran(cacheManager.beginTransation(), thisCacheMgr);
		return new UserTran(cacheManager.beginUserTransation(), cacheManager);
	}
	
	
	public Table createTable(String tableName, String keyName, List<String> columnNameList, List<BasicType> tList,
			Transaction thisTran, int M) throws IOException, ClassNotFoundException 
	{
		ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);

		byte[] bytes = new byte[Page.PAGE_SIZE];
		
		ByteArrayOutputStream byt = new ByteArrayOutputStream();

		LockType lock = LockType.Shared;

		ObjectOutputStream obj = new ObjectOutputStream(byt);
		obj.writeObject(tableName);
		obj.writeObject(keyName);
		obj.writeObject(columnNameList);
		
		String s1;
		List<String>list1=new ArrayList<String>();
		
		for(BasicType b:tList)
		{
			s1=b.toString();
			list1.add(s1);
		}
		
		obj.writeObject(list1);
		List<Integer>list2=new ArrayList<Integer>();
		obj.writeObject(list2);
		Integer i=-1;
		obj.writeObject(i);
		
		Page tablePage = new Page(tempBuffer);
		
		Integer ii=tablePage.getPageID();
		obj.writeObject(ii);
		
		Integer iii=M;
		obj.writeObject(iii);
		
		List<Row>list3=new ArrayList<Row>();
		obj.writeObject(list3);
		
		bytes = byt.toByteArray();
		
		tempBuffer.rewind();
		
		tempBuffer.put(bytes);
		
		cacheManager.writePage(thisTran.tranNum, tablePage);

		Integer pageID = tablePage.getPageID();
		
		if (!tableName.equals("master")) 
		{
			List<String> masterRow_s = new ArrayList<String>();
			masterRow_s.add(tableName);
			masterRow_s.add(pageID.toString());
			masterRow_s.add("-1");
			
			Row r1=new Row(masterRow_s);
			
			Node master=new Node(-2,cacheManager,thisTran);
			master.insertRow(r1, thisTran);
			/*Cursor masterCursor = master.createCursor(thisTran);
			masterCursor.insert(thisTran, masterRow_s);去掉*/
		}
		return new Table(pageID, cacheManager, thisTran); 
	}


	public Table createTable(String tableName, String keyName, List<String> columnNameList, List<BasicType> tList,
			Transaction thisTran) throws IOException, ClassNotFoundException 
	{
		
		int i2;
		
		int search_position=-1000;
		
		Node n1=new Node(-2,cacheManager,thisTran);
		
		Cell c2=new Cell(tableName);
		
		while(true)
    	{
    		for(i2=0;i2<n1.rowList.size();i2++)
    		{
    			if(n1.rowList.get(i2).getCell(0).bigerThan(c2))
    			{
    				break;
    			}
    		}
    		
    		if(i2>0&&n1.rowList.get(i2-1).getCell(0).equalTo(c2))
    		{
    			search_position=i2-1;
    			break;
    		}
    		else {
    			if(i2<n1.sonNodeList.size())
    			{
    				n1=new Node(n1.sonNodeList.get(i2),cacheManager,thisTran);
    			}
    			else {
    				break;
    			}
    		}
    	}
    	//查master表
    	if(search_position!=-1000)
		{
    		if(n1.rowList.get(search_position).getCell(0).getValue_s().contentEquals(tableName))
    		{
    			System.out.println(tableName+"表已经建立在master表中，具体如下：");
    			System.out.println("表所在页："+n1.rowList.get(search_position).getCell(1).getValue_s());
    			System.out.println("表索引表所在页（为-1代表还没有索引表）："+n1.rowList.get(search_position).getCell(2).getValue_s());
    			Node nn1=new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()), cacheManager, thisTran);
    			System.out.println("主键："+nn1.keyName);
    			int iiii=0;
    			for(String s1:nn1.columnname)
    			{
    				iiii++;
    				System.out.println("第"+iiii+"个列名："+s1);
    				System.out.println("第"+iiii+"个列属性："+nn1.columntype.get(iiii-1));
    			}
    			
    			System.out.println();
    			return null;
    		}
		}
		
		ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);

		byte[] bytes = new byte[Page.PAGE_SIZE];
		ByteArrayOutputStream byt = new ByteArrayOutputStream();

		LockType lock = LockType.Shared;

		List<Column> columns = new ArrayList<Column>();
		
		for (int i = 0; i < columnNameList.size(); i++) 
		{
			Column tempColumn = new Column(tList.get(i), columnNameList.get(i), 0);
			columns.add(tempColumn);
		}

		ObjectOutputStream obj = new ObjectOutputStream(byt);
		obj.writeObject(tableName);

		obj.writeObject(keyName);
		
		List<String>list100=new ArrayList<String>();
		
		List<String>list101=new ArrayList<String>();
		
		for(Column c1: columns )
		{
			list100.add(c1.getName());
			list101.add(c1.getType().toString());
		}
		
		obj.writeObject(list100);
		
		obj.writeObject(list101);

		List<Integer> list200=new ArrayList<Integer>();
		
		obj.writeObject(list200);
		
		Integer i300=-1;
		
		obj.writeObject(i300);
		
		Page tablePage = new Page(tempBuffer);
		
		Integer self_page=tablePage.getPageID();
		
		obj.writeObject(self_page);
		
		Integer i400=this.M;
		
		obj.writeObject(i400);
		
		List<Row> list500=new ArrayList<Row>();
		
		obj.writeObject(list500);
		
		//obj.writeObject(lock);
		//obj.writeObject(-1);
		bytes = byt.toByteArray();
		tempBuffer.put(bytes);

		byte[] bytess = new byte[Page.PAGE_SIZE];
		tempBuffer.rewind();
		tablePage.getPageBuffer().get(bytess, 0, tablePage.getPageBuffer().remaining());

		cacheManager.writePage(thisTran.tranNum, tablePage);

		// thisTran.Commit();
		Integer pageID = tablePage.getPageID();

		if (!tableName.equals("master")) 
		{
			List<String> masterRow_s = new ArrayList<String>();
			masterRow_s.add(tableName);
			masterRow_s.add(pageID.toString());
			masterRow_s.add("-1");

			Row r1=new Row(masterRow_s);
			Node master=new Node(-2,cacheManager,thisTran);
			master.insertRow(r1, thisTran);
			/*Cursor masterCursor = master.createCursor(thisTran);
			masterCursor.insert(thisTran, masterRow_s);去掉*/
		}
		return new Table(pageID, cacheManager, thisTran); // NULL
	}

	public View createView(List<String> sList, List<BasicType> tList, List<List<String>> rowStringList,
			Transaction thisTran) {
		return new View(sList, tList, rowStringList);
	}

	/*public boolean dropTable(String tableName, Transaction thisTran) throws IOException, ClassNotFoundException {
		Cursor masterCursor = master.createCursor(thisTran);
		//masterCursor.moveToUnpacked(thisTran, tableName);
		int pageID = masterCursor.getCell_i("pageNumber");
		Table thistable = new Table(pageID, cacheManager, thisTran);

//        thistable.getRootNode(thisTran).drop(thisTran);
		Node rootnode = thistable.getRootNode(thisTran);
		if (rootnode != null) {
			rootnode.drop(thisTran);
		}

		cacheManager.deletePage(thisTran.tranNum, pageID);
		masterCursor.delete(thisTran);
		return true;
	}*/

	// 鍒犻櫎涓�寮犺〃
	/*public boolean dropTable(Table table, Transaction thisTran) throws IOException, ClassNotFoundException {
		return dropTable(table.tableName, thisTran);
	}*/

	// 鏍规嵁浼犳潵鐨勮〃鍚嶈繑鍥濼able琛ㄥ璞�
	public Table getTable(String tableName, Transaction thisTran) throws IOException, ClassNotFoundException {
		//masterCursor.moveToUnpacked(thisTran, tableName);原来的作修改
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
		return new Table(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()), cacheManager, thisTran);
	}

	// 缁欐暣涓暟鎹簱涓殑琛ㄥ叏閮ㄥ姞閿�
	/*public boolean lock(Transaction thisTran) throws IOException, ClassNotFoundException {
		if (master.isLocked()) {
			return false;
		} else {
			Cursor masterCursor = master.createCursor(thisTran);
			do {
				Table temp = new Table(masterCursor.getCell_i("pageNumber"), cacheManager, thisTran);
				temp.lock(thisTran);
			} while (masterCursor.moveToNext(thisTran));
			master.lock(thisTran);
			return true;
		}
	}

	// 缁欐暟鎹簱涓叏閮ㄧ殑琛ㄨВ閿�
	public boolean unLock(Transaction thisTran) throws IOException, ClassNotFoundException {
		if (master.isLocked()) {
			master.unLock(thisTran);
			Cursor masterCursor = master.createCursor(thisTran);
			do {
				Table temp = new Table(masterCursor.getCell_i("pageNumber"), cacheManager, thisTran);
				temp.unLock(thisTran);
			} while (masterCursor.moveToNext(thisTran));
			return true;
		} else {
			return true;
		}
	}*/

}
