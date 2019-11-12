package npu.zunsql.cache;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import npu.zunsql.treemng.*;



public class CacheMgr {
	protected static final int CacheCapacity = 20;
	protected String dbName = null;
	public static final int FILEHEADERSIZE = 1024;
	public static final int UNUSEDLISTSIZE = 1024;

	public List<Row> delete_update=null;
	
	public List<Row> search_result=null;
	
	public List<String> search_result_final=null;
	
	
	public boolean is_not_first_read_unlist=false;//flase意味着从来没有读过硬盘中的这一部分，true已经读过一次，true并且说明只有内存中的才是新值。
	
	
	///////////////////////////////////////////////////////////////
	
	//Map<Integer, List<Page>> transOnPage是第一层cache，当这一层cache满之后，会由CacheMgr
	//commitTransation方法判断是否该事务对应的页数大于100，如果大于，则写回，但写回的部分并没有写回内存，
	//而是写入了public List<Page> cacheList = null;中，这是第二层cache，当第二层cache满后，
	//用最近最少使用来继续写回到硬盘。
	
	/////////////////////////////////////////////////////////
	
	
	
	//cacheList和cachePageMap和读策略有关，并与最近最少使用策略有关
	// store the page
	public List<Page> cacheList = null;
	
	// make the ID map the cache，不是键值的内容不需要使用，可以任意赋值，作用是判断一页是否
	//在cacheList中
	public ConcurrentMap<Integer, Integer> cachePageMap = null;
	//cacheList和cachePageMap和读策略有关，并与最近最少使用策略有关
	
	
	//transOnPage和写策略有关
	// record the transaction ID which has made change on the page
	public Map<Integer, List<Page>> transOnPage = null;
	//transOnPage和写策略有关
	
	
	//要把delete之后的页收集起来，rowList的项数大于51，则只有前50项是有用的，第51项指向下一个可分配页的页号
	// record the number of the block which stores the unusedList_count
	public List<Integer> unusedList_PageID = null;
	//要把delete之后的页收集起来，
	
	
	
	
	
	
	
	
	
	
	
	// make the ID map the transaction
	protected Map<Integer, Transaction> transMgr = null;
	
	private ReadWriteLock lock;

//	protected UserTransaction userTrans = null;
	protected Map<Integer, UserTransaction> userTransMgr = null;
//	protected List<Page> userTransPages = null;
	protected List<Integer> userTransList = null;

	public CacheMgr(String dbName) 
	{
		this.search_result=new ArrayList<Row>();
		this.search_result_final=new ArrayList<String>();
		this.delete_update=new ArrayList<Row>();
		this.dbName = dbName;
		this.cacheList = new ArrayList<Page>();
		this.cachePageMap = new ConcurrentHashMap<Integer, Integer>();
		this.transMgr = new HashMap<Integer, Transaction>();
		this.transOnPage = new HashMap<Integer, List<Page>>();
		this.unusedList_PageID = new ArrayList<Integer>();
		this.lock = new ReentrantReadWriteLock();

//		this.userTrans = null;
		this.userTransMgr = new HashMap<Integer, UserTransaction>();
//		this.userTransPages = null;
		this.userTransList = null;
	}
//这是自己写的，这是修改的
	public void search_result(Row r1)
  	{
		/////////////////每次search之前都要把search_result清空
/////////////////每次search之前都要把search_result清空
/////////////////每次search之前都要把search_result清空
		this.search_result.add(r1);
	}
	
	public boolean isNew() throws ClassNotFoundException 
	{
		
		int i;
		
		File db_file = new File(this.dbName);
		FileChannel fc = null;
		// if db_file has existed,use the API to read the file
		if (db_file.exists()) 
		{
			
			RandomAccessFile fin = null;
			
			/*File file = new File(this.dbName);
			RandomAccessFile fin = new RandomAccessFile(file, "rw");
			fc = fin.getChannel();
			// share lock
			FileLock lock = fc.lock(0, Long.MAX_VALUE, true);
			ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
			fc.read(tempBuffer, CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + pageID * Page.PAGE_SIZE);
			tempPage = new Page(pageID, tempBuffer);
			lock.release();
			fin.close();*/
			
			try {
				fin = new RandomAccessFile(db_file, "rw");
				fc = fin.getChannel();
				ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
				fc.read(fileHeader, 0);
				
				byte [] bytes=new byte[Page.PAGE_SIZE];
			    fileHeader.rewind();
			    fileHeader.get(bytes,0,fileHeader.remaining());
			       
			    ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
		        ObjectInputStream objTable=new ObjectInputStream(byteTable);
		        
		        Integer i1;
		        Integer ii2;
		        i1=(Integer)objTable.readObject();
		        Page.pageCount=i1;
			    ii2=(Integer)objTable.readObject();
		        
		        fileHeader.clear();
		        fileHeader.rewind();
		        
		        fc.read(fileHeader,CacheMgr.FILEHEADERSIZE);
		        
		        fileHeader.rewind();
		        fileHeader.get(bytes,0,fileHeader.remaining());
		        
		        byteTable=new ByteArrayInputStream(bytes);
		        objTable=new ObjectInputStream(byteTable);
		        
		        i1=(Integer)objTable.readObject();
		        
		        int flag2;
		        
		        this.unusedList_PageID.clear();
		        
		        if(i1!=-1)
		        {
			        while(true)
			        {
			        	flag2=0;
			        	for(i=0;i<50;i++)
			        	{
			        		i1=(Integer)objTable.readObject();
			        		if(i1!=-1)
			        		{
			        			this.unusedList_PageID.add(i1);
			        		}
			        		else {
			        			flag2=1;
			        			break;
			        		}
			        	}
			        	
			        	if(flag2==1)
			        	{
			        		break;
			        	}
			        	
			        	else {
			        		i1=(Integer)objTable.readObject();
			        		
			        		fileHeader.clear();
					        fileHeader.rewind();
					        fc.read(fileHeader,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + i1* Page.PAGE_SIZE);
					        
					        fileHeader.rewind();
					        fileHeader.get(bytes,0,fileHeader.remaining());
					       
					        byteTable=new ByteArrayInputStream(bytes);
					        objTable=new ObjectInputStream(byteTable);
			        	}
		
			        }
		        }
		        
		        fin.close(); 
		        
		        
		        System.out.println(this.dbName+"已经被创建过，具体如下：");
		        System.out.println("达到的最大页号："+Page.pageCount);
		        System.out.println("master表B树根节点页号："+ii2);
		        System.out.println("页大小："+Page.PAGE_SIZE);
		        System.out.println("空闲页数："+this.unusedList_PageID.size());
				System.out.println();
		        
		        return false;
			}
			catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		} 
		else {
			try {
				
				Page.pageCount=0;
				
				db_file.createNewFile();
				RandomAccessFile fin = null;
				fin = new RandomAccessFile(db_file, "rw");
				fc = fin.getChannel();
				ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
				
				fileHeader.rewind();
				
				Integer ii1;//已经分配到最大的页序号，再分配从这开始分配
				
				Integer ii2;//master表B树根节点所在页号，初始化0
				
				ii1=0;
				
				ii2=0;
				
				ByteArrayOutputStream o2=new ByteArrayOutputStream();
				ObjectOutputStream ob2=new ObjectOutputStream(o2);
				
				ob2.writeObject(ii1);
				
				ob2.writeObject(ii2);
				
				fileHeader.rewind();
				
				fileHeader.put(o2.toByteArray());
				
				fileHeader.rewind();				
				fc.write(fileHeader, 0);
				
				
				ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
				ByteArrayOutputStream o1=new ByteArrayOutputStream();
				ObjectOutputStream ob1=new ObjectOutputStream(o1);
				Integer i1=-1;//指是否有可用页，-1表示没有，1表示有
				
				ob1.writeObject(i1);
				
				unusedListBuffer.rewind();
				unusedListBuffer.put(o1.toByteArray());
				
				unusedListBuffer.rewind();			
				fc.write(unusedListBuffer, CacheMgr.FILEHEADERSIZE);

				fin.close();
			} 
			catch (FileNotFoundException e) 
			{
				e.printStackTrace();
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			return true;
		}
		return true;
	}

	public void close() throws ClassNotFoundException 
	{
		File db_file = new File(this.dbName);
		FileChannel fc = null;
		RandomAccessFile fin = null;
		try {
			
			int i;
			
			fin = new RandomAccessFile(db_file, "rw");
			fc = fin.getChannel();
			
			Page p2;
			
			while(cacheList.size()!=0)
			{
				p2=cacheList.get(0);
				FileLock lock = fc.lock();
				p2.pageBuffer.rewind();
				fc.write(p2.pageBuffer,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + p2.pageID * Page.PAGE_SIZE);
				lock.release();
				cacheList.remove(0);
				cachePageMap.remove(p2.pageID);
			}
			
			List<Page>w1=this.transOnPage.get(0);
			
			while(w1!=null&&w1.size()!=0)
			{
				p2=w1.get(0);
				FileLock lock = fc.lock();
				p2.pageBuffer.rewind();
				fc.write(p2.pageBuffer,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + p2.pageID * Page.PAGE_SIZE);
				lock.release();
				w1.remove(0);
			}
			
			int flag=0;
			Integer i1,i2=0/*当前页*/,i3=0/*之后空闲页*/;
			ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
			
			
			if(this.unusedList_PageID.size()==0)//
				///第二个cache错误找到的解决，要写到报告
				///第二个cache错误找到的解决，要写到报告
				///第二个cache错误找到的解决，要写到报告
				///第二个cache错误，要写到报告
				///第二个cache错误找到的解决，要写到报告
				///第二个cache错误，要写到报告
				///第二个cache错误找到的解决，要写到报告
				///第二个cache错误，要写到报告
			{
				ByteArrayOutputStream b100=new ByteArrayOutputStream();
				ObjectOutputStream o100=new ObjectOutputStream(b100);
				i1=-1;
				o100.writeObject(i1);
				
				
				buffer.rewind();
				buffer.put(b100.toByteArray());
				
				buffer.rewind();
				fc.write(buffer,CacheMgr.FILEHEADERSIZE );
				
			}
			else {
				
				while(this.unusedList_PageID.size()!=0)
				{
					
					ByteArrayOutputStream b100=new ByteArrayOutputStream();
					ObjectOutputStream o100=new ObjectOutputStream(b100);
				
					if(flag==0)
					{
						i1=1;//代表有空闲页
						o100.writeObject(i1);
					}
					
					for(i=0;i<50&&this.unusedList_PageID.size()!=0;i++)
					{
						o100.writeObject(this.unusedList_PageID.get(0));
						this.unusedList_PageID.remove(0);
					}
					
					if(this.unusedList_PageID.size()!=0)
					{
						i1=this.unusedList_PageID.get(0);
						
						o100.writeObject(i1);
						
						i3=i1;
						
					}
					
					else {
						i1=-1;
						o100.writeObject(i1);//代表终结
					}
					
					buffer.rewind();
					buffer.put(b100.toByteArray());
					
					if(flag==0)
					{
						buffer.rewind();
						fc.write(buffer,CacheMgr.FILEHEADERSIZE );
						i2=i3;
					}
					
					else {
						buffer.rewind();
						fc.write(buffer,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + i2 * Page.PAGE_SIZE);
						i2=i3;
					}
					
					flag=1;
					
				}
			
			}
			
			ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
			
			fileHeader.rewind();
			
			Integer ii1;//已经分配到最大的页序号，再分配从这开始分配
			
			Integer ii2;//master表根节点B树所在页
			
			ii1=Page.pageCount;
			
			ByteBuffer fileHeader2 = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
			fc.read(fileHeader2, 0);
			
			byte [] bytes2=new byte[Page.PAGE_SIZE];
		    fileHeader2.rewind();
		    fileHeader2.get(bytes2,0,fileHeader.remaining());
		       
		    ByteArrayInputStream byteTable2=new ByteArrayInputStream(bytes2);
	        ObjectInputStream objTable2=new ObjectInputStream(byteTable2);
	        
	        objTable2.readObject();
	        
	        ii2=(Integer)objTable2.readObject();
	        
			
			ByteArrayOutputStream o2=new ByteArrayOutputStream();
			ObjectOutputStream ob2=new ObjectOutputStream(o2);
			
			ob2.writeObject(ii1);
			
			ob2.writeObject(ii2);
			
			fileHeader.rewind();
			
			fileHeader.put(o2.toByteArray());
			
			fileHeader.rewind();				
			fc.write(fileHeader, 0);
			
			fin.close();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}

	/**
	 * start a new transaction and return transID new a objection and get the lock
	 * record the transMgr
	 **/
	public int beginTransation(String s) 
	{
		Transaction trans = new Transaction(s, lock);
		trans.begin();
		this.transMgr.put(trans.transID, trans);
		return trans.transID;
	}

	public int beginUserTransation() 
	{
		UserTransaction trans = new UserTransaction(lock);
		trans.begin();
//		userTrans = trans;
//		this.userTransPages = new ArrayList<>();
		this.userTransList = new ArrayList<>();
		this.userTransMgr.put(trans.transID, trans);
		return trans.transID;
	}

	/**
	 * commit the transaction and update the cache 1.get the transonPage 2.read the
	 * page which will be changed and record it to the journal 3.if hit cache , then
	 * update it 4.write the new one to the page 5.if not hit,then use LRU to
	 * replace one page
	 */
	public boolean commitTransation(int transID) throws IOException 
	{
		Transaction trans = transMgr.get(transID);
		if (trans.WR) 
		{
			List<Page> writePageList = transOnPage.get(transID);
			File db_file = new File(this.dbName);
			try {
				if (writePageList != null&&writePageList.size()>20) 
				{
					Page p2;
					Page copyPage;
					while(writePageList.size()>15)
					{
						
						p2=writePageList.get(0);
						
						if(cachePageMap.get(p2.pageID)!=null)
						{
							for(int j=0;j<cacheList.size();j++)
							{
								copyPage = cacheList.get(j);
								if(copyPage.pageID==p2.pageID)
								{
									cacheList.remove(j);
									break;
								}
							}
							cacheList.add(p2);
						}
						
						else if(cacheList.size()<20)
						{
							cacheList.add(p2);
							cachePageMap.put(p2.pageID,1);
						}
						
						else {
							cacheList.add(p2);
							cachePageMap.put(p2.pageID,1);
							
							while(cacheList.size()>15)
							{
								copyPage=cacheList.get(0);
								if (db_file.exists() && db_file.isFile()) 
								{
									RandomAccessFile fin = new RandomAccessFile(db_file, "rw");
									FileChannel fc = fin.getChannel();
									FileLock lock = fc.lock();
									copyPage.pageBuffer.rewind();
									fc.write(copyPage.pageBuffer,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + copyPage.pageID * Page.PAGE_SIZE);
									lock.release();
									fin.close();
								}
								cacheList.remove(0);
								cachePageMap.remove(copyPage.pageID);
								
								copyPage=null;
								
							}
							
						}
						
						writePageList.remove(0);
						p2=null;
					}
				}
			} 
			finally {
			}
		}

		if (userTransList != null) 
		{
			userTransList.add(transID);
		} 
		else {
//			File journal_file = new File(Integer.toString(transID) + "-journal");
//			journal_file.delete();
		}

		return true;
	}

	public boolean commitUserTransation(int transID) throws IOException 
	{
		UserTransaction trans = userTransMgr.get(transID);

		trans.commit();
		int index = (int) (userTransList.size()) - 1;

//		for (; index >= 0; --index) {
//			File journal_file = new File(Integer.toString(userTransList.get(index)) + "-journal");
//			journal_file.delete();
//		}

		this.userTransList = null;

		return true;
	}

	/**
	 * roll back the transaction lease the lock and do not affect cache
	 **/
	
	/**
	 * use transID to read pageID and return page_copy if this page has been stored
	 * in the cacheList ,then directly return it else try to get it from file: if
	 * cacheList is full , use LRU to replace one page else , add this page into
	 * cacheList
	 * @throws IOException 
	 **/
	public Page readPage(int transID, int pageID) throws IOException 
	{
		List<Page> writePageList = transOnPage.get(transID);

		// test if this page has been written by this transaction but not commit
		if (writePageList != null) 
		{
			for (int i = 0; i < writePageList.size(); i++) 
			{
				Page copyPage = writePageList.get(i);
				if (copyPage.pageID == pageID)
				{
					return copyPage;
				}
			}
		}
		

		
		
		
		Page tempPage;
		Integer tempPage2;
		tempPage2 = this.cachePageMap.get(pageID);
		
		// 第一层cache not hit，第二层cache
		if (tempPage2 == null) 
		{
			// 第二层cache has been full , use LRU to replace it，第0个是最久最后一个最近
			if (this.cacheList.size() > CacheMgr.CacheCapacity) 
			{	
				File db_file = new File(this.dbName);
				while(cacheList.size()>15)
				{
					tempPage = this.cacheList.get(0);
					
					if (db_file.exists() && db_file.isFile()) 
					{
						RandomAccessFile fin = new RandomAccessFile(db_file, "rw");
						FileChannel fc = fin.getChannel();
						FileLock lock = fc.lock();
						tempPage.pageBuffer.rewind();
						fc.write(tempPage.pageBuffer,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + tempPage.pageID * Page.PAGE_SIZE);
						lock.release();
						fin.close();
					}
					
					this.cacheList.remove(0);
					this.cachePageMap.remove(tempPage.pageID);
				}

				tempPage = getPageFromFile(pageID);
				this.cacheList.add(tempPage);
				this.cachePageMap.put(pageID, 1);
			} 
			else {
				tempPage = getPageFromFile(pageID);
				this.cacheList.add(tempPage);
				this.cachePageMap.put(pageID, 1);
			}
			
			return tempPage;

		}
		// 第二层cache hit
		else {
			Page jPage=null;
			// use LRU to update the cacheList
			for (int j = 0; j < cacheList.size(); j++)//有这一页
			{
				jPage = cacheList.get(j);
				if (jPage.pageID == pageID) 
				{
					cacheList.remove(j);
					break;
				}
			}
			this.cacheList.add(jPage);
			return jPage;
		}
	}

	/**
	 * use transID to get the write queue and add it to writePagelist Attention:one
	 * transaction can write more than one page,so we should use a list to store it
	 * @throws IOException 
	 **/
	public boolean writePage(int transID, Page tempBuffer) throws IOException 
	{
		List<Page> writePageList = transOnPage.get(transID);
		if (writePageList == null) 
		{
			writePageList = new ArrayList<Page>();
			writePageList.add(tempBuffer);
			transOnPage.put(transID, writePageList);
		} 
		else if(writePageList.size()<20) /////////////////////
			/////////////////////////////////////
			///////////////////////
			//////第一个cache错误找到的解决，写到报告上
			/////////////////////////////////////写报告写上
		{
			int flag=0;
			for(int i=0;i<writePageList.size();i++)
			{
				if(writePageList.get(i).pageID==tempBuffer.pageID)
				{
					flag=1;
					writePageList.remove(i);
					writePageList.add(tempBuffer);
					break;
				}
			}
			if(flag==0)
			{
				writePageList.add(tempBuffer);
			}
		}
		else {
			
			int flag=0;//没有这一页
			for(int i=0;i<writePageList.size();i++)
			{
				if(writePageList.get(i).pageID==tempBuffer.pageID)
				{
					flag=1;//有这一页
					writePageList.remove(i);
					writePageList.add(tempBuffer);
					break;
				}
			}
			if(flag==0)
			{//没有这一页
				writePageList.add(tempBuffer);
			}
			
			File db_file = new File(this.dbName);
			
			while(writePageList.size()>15)
			{
				Page copyPage = writePageList.get(0);
				
				if(cachePageMap.get(copyPage.pageID)!=null)
				{//有这一页
					for (int j = 0; j < cacheList.size(); j++) 
					{
						Page jPage = cacheList.get(j);
						if (jPage.pageID == copyPage.pageID) 
						{
							cacheList.remove(j);
							cacheList.add(copyPage);
							break;
						}
					}
				}
				
				else if(cacheList.size()<20) //完全没有这一页
				{
					cacheList.add(copyPage);
					cachePageMap.put(copyPage.pageID, 1);
				}
				
				
				else {//完全没有这一页
		
					cacheList.add(copyPage);
					cachePageMap.put(copyPage.pageID, 1);
					
					Page p2;
					while(cacheList.size()>15)
					{
						p2=cacheList.get(0);
						if (db_file.exists() && db_file.isFile()) 
						{
							RandomAccessFile fin = new RandomAccessFile(db_file, "rw");
							FileChannel fc = fin.getChannel();
							FileLock lock = fc.lock();
							p2.pageBuffer.rewind();
							fc.write(p2.pageBuffer,CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + p2.pageID * Page.PAGE_SIZE);
							lock.release();
							fin.close();
						}
						cacheList.remove(0);
						cachePageMap.remove(p2.pageID);
						p2=null;
					}
				}
				
				writePageList.remove(0);
				
				copyPage=null;
				
			}
		} 

		return true;
	}
	
	/**
	 * Add the unused pageID into the Page.unusedID list
	 **/
	public void deletePage(int transID, int pageID) {
		Page.unusedID.add(pageID);
	}

	/**
	 * write page to file
	 **/
	private boolean setPageToFile(Page tempPage, File file) 
	{
		FileChannel fc = null;
		try {
			if (!file.exists()) 
			{
				file.createNewFile();
			}
			RandomAccessFile fin = new RandomAccessFile(file, "rw");
			fc = fin.getChannel();
			// write lock
			FileLock lock = fc.lock();
			tempPage.pageBuffer.rewind();
			fc.write(tempPage.pageBuffer,
					CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + tempPage.pageID * Page.PAGE_SIZE);
			lock.release();
			fin.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		} 
		finally {
			try {
				if (fc != null) 
				{
					fc.close();
				}
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * Read the Page from the file using pageID
	 **/
	private Page getPageFromFile(int pageID) 
	{
		Page tempPage = null;
		FileChannel fc = null;
		try {
			File file = new File(this.dbName);
			RandomAccessFile fin = new RandomAccessFile(file, "rw");
			fc = fin.getChannel();
			// share lock
			//FileLock lock = fc.lock(0, Long.MAX_VALUE, true);
			ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
			tempBuffer.clear();
			tempBuffer.rewind();
			fc.read(tempBuffer, CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + pageID * Page.PAGE_SIZE);
			tempBuffer.rewind();
			tempPage = new Page(pageID, tempBuffer);
			//lock.release();
			fin.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		finally {
			try {
				if (fc != null) 
				{
					fc.close();
				}
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		return tempPage;
	}
}
