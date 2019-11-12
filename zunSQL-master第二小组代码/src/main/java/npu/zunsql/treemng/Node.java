package npu.zunsql.treemng;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
/**
 * Created by WQT on 2017/11/6.
 */
public class Node {
	public Integer left_or_right=0;
	
	public Integer adjust_position=0;
	
	public Integer search_position=0;
	
	public Integer counter=0;
	
	public Integer stack_auto_top=0;
	
	public Node rhizine;
	
	public Node[] stack_auto;
	
	public String node_tablename;
	
	public String keyName;
	
	public List<String> columnname;
	
	public List<String> columntype;
	
    public List<Integer> sonNodeList;

    public Integer fatherNodeID;//值是-1时表示没有父节点

    public int order;//没有用处的定义

    public List<Row> rowList;

    public Integer M = 3;//就是树节点的阶

    public CacheMgr cacheManager;

    public Integer pageOne;//就是当前节点所在的页号

    public Node(int thisPageID, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
        
    	if(thisPageID!=-2)
    	{
	    	this.cacheManager = cacheManager;
	        Page ppp=this.cacheManager.readPage(thisTran.tranNum, thisPageID);
	        this.pageOne=ppp.getPageID();
	        
	        ByteBuffer thisBufer = ppp.getPageBuffer();
	        byte [] bytes=new byte[Page.PAGE_SIZE] ;
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());
	
	        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
	        ObjectInputStream objTable=new ObjectInputStream(byteTable/*new BufferedInputStream(new ByteArrayInputStream(bytes))*/);
	
	        this.node_tablename=(String)objTable.readObject();
	        this.keyName=(String)objTable.readObject();
	        this.columnname=(List<String>)objTable.readObject();
	        this.columntype=(List<String>)objTable.readObject();
	        this.sonNodeList=(List<Integer>)objTable.readObject();
	        this.fatherNodeID=(Integer) objTable.readObject();//用-1代表没有父亲
	        objTable.readObject();//本身节点的页号
	        this.M=(Integer)objTable.readObject();
	        this.order=this.M;//没有用到的地方
	        this.rowList=(List<Row>)objTable.readObject();
	        this.rhizine=this;
    	}
    	
    	else if(thisPageID==-2)//表示读的是master表的B树根节点的页号
    	{
    		this.cacheManager = cacheManager;
	        Page ppp=this.cacheManager.readPage(thisTran.tranNum, thisPageID);
	        ByteBuffer thisBufer = ppp.getPageBuffer();
	        byte [] bytes=new byte[Page.PAGE_SIZE] ;
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());
	
	        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
	        ObjectInputStream objTable=new ObjectInputStream(byteTable);
	        
	        objTable.readObject();
	        this.pageOne=(Integer)objTable.readObject();
	        
	        ppp=this.cacheManager.readPage(thisTran.tranNum, this.pageOne);
	        thisBufer = ppp.getPageBuffer();
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());
	        
	        byteTable=new ByteArrayInputStream(bytes);
	        objTable=new ObjectInputStream(byteTable);
	        
	        this.node_tablename=(String)objTable.readObject();
	        this.keyName=(String)objTable.readObject();
	        this.columnname=(List<String>)objTable.readObject();
	        this.columntype=(List<String>)objTable.readObject();
	        this.sonNodeList=(List<Integer>)objTable.readObject();
	        this.fatherNodeID=(Integer) objTable.readObject();//用-1代表没有父亲
	        objTable.readObject();//本身节点的页号
	        this.M=(Integer)objTable.readObject();
	        this.order=this.M;//没有用到的地方
	        this.rowList=(List<Row>)objTable.readObject();
	        this.rhizine=this;
    	}
    }
    
    public Node(int thisPageID, CacheMgr cacheManager) throws IOException, ClassNotFoundException {
        
    	if(thisPageID!=-2)
    	{
	    	this.cacheManager = cacheManager;
	        Page ppp=this.cacheManager.readPage(0, thisPageID);
	        this.pageOne=ppp.getPageID();
	        
	        ByteBuffer thisBufer = ppp.getPageBuffer();
	        byte [] bytes=new byte[Page.PAGE_SIZE] ;
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());
	
	        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
	        ObjectInputStream objTable=new ObjectInputStream(byteTable/*new BufferedInputStream(new ByteArrayInputStream(bytes))*/);
	
	        this.node_tablename=(String)objTable.readObject();
	        this.keyName=(String)objTable.readObject();
	        this.columnname=(List<String>)objTable.readObject();
	        this.columntype=(List<String>)objTable.readObject();
	        this.sonNodeList=(List<Integer>)objTable.readObject();
	        this.fatherNodeID=(Integer) objTable.readObject();//用-1代表没有父亲
	        objTable.readObject();//本身节点的页号
	        this.M=(Integer)objTable.readObject();
	        this.order=this.M;//没有用到的地方
	        this.rowList=(List<Row>)objTable.readObject();
	        this.rhizine=this;
    	}
    	
    	else if(thisPageID==-2)//表示读的是master表的B树根节点的页号
    	{
    		this.cacheManager = cacheManager;
	        Page ppp=this.cacheManager.readPage(0, thisPageID);
	        ByteBuffer thisBufer = ppp.getPageBuffer();
	        byte [] bytes=new byte[Page.PAGE_SIZE] ;
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());
	
	        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
	        ObjectInputStream objTable=new ObjectInputStream(byteTable);
	        
	        objTable.readObject();
	        this.pageOne=(Integer)objTable.readObject();
	        
	        ppp=this.cacheManager.readPage(0, this.pageOne);
	        thisBufer = ppp.getPageBuffer();
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());
	        
	        byteTable=new ByteArrayInputStream(bytes);
	        objTable=new ObjectInputStream(byteTable);
	        
	        this.node_tablename=(String)objTable.readObject();
	        this.keyName=(String)objTable.readObject();
	        this.columnname=(List<String>)objTable.readObject();
	        this.columntype=(List<String>)objTable.readObject();
	        this.sonNodeList=(List<Integer>)objTable.readObject();
	        this.fatherNodeID=(Integer) objTable.readObject();//用-1代表没有父亲
	        objTable.readObject();//本身节点的页号
	        this.M=(Integer)objTable.readObject();
	        this.order=this.M;//没有用到的地方
	        this.rowList=(List<Row>)objTable.readObject();
	        this.rhizine=this;
    	}
    }

    public Node(int order2,String tablename2,String keyname2,List<String>list1,List<String>list2,CacheMgr cacheManager, Transaction thisTran) throws IOException {
    	ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        this.node_tablename=tablename2;
        this.keyName=keyname2;//只起占位作用
        this.columnname=list1;//只起占位作用
        this.columntype=list2;//只起占位作用
        this.sonNodeList = new ArrayList<Integer>();
        this.fatherNodeID = -1;//用-1代表没有父亲
        this.cacheManager = cacheManager;
        
        Page ppp=new Page(buffer);
        
        this.pageOne = ppp.getPageID();
        this.M = order2;
        this.order=this.M;//没有用到的地方
        this.rowList=new ArrayList<Row>();
        this.rhizine=this;
        
        
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);
        obj.writeObject(node_tablename);
        obj.writeObject(keyName);
        obj.writeObject(columnname);
        obj.writeObject(columntype);
        obj.writeObject(sonNodeList);
        Integer i=fatherNodeID;
        obj.writeObject(i);
        Integer ii=pageOne;
        obj.writeObject(ii);
        Integer i2=M;
        obj.writeObject(i2);
        obj.writeObject(rowList);
        buffer.rewind();
        buffer.put(byt.toByteArray());
        cacheManager.writePage(thisTran.tranNum,ppp);
    }
   

    public void intoBytes (Transaction thisTran) throws IOException {
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);

        
        obj.writeObject(node_tablename);
        obj.writeObject(keyName);
        obj.writeObject(columnname);
        obj.writeObject(columntype);
        obj.writeObject(sonNodeList);
        Integer i=fatherNodeID;
        obj.writeObject(i);
        Integer ii=pageOne;
        obj.writeObject(ii);
        Integer i2=M;
        obj.writeObject(i2);
        obj.writeObject(rowList);
        Page ppp=this.cacheManager.readPage(thisTran.tranNum, pageOne);
        ppp.getPageBuffer().rewind();
        ppp.getPageBuffer().put(byt.toByteArray());
        cacheManager.writePage(thisTran.tranNum,ppp);
    }
    
    public void update_root(Integer page,String name,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int search_position=100;
    	Node n1=new Node(-2,cacheManager,thisTran);//代表访问master表的B树根节点
    	Cell c1=new Cell(name);
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
    	
    	List<String> s1=new ArrayList<String>();
    	
    	s1.add(name);
    	s1.add(page.toString());
    	s1.add(n1.rowList.get(search_position).getCell(2).getValue_s());
    	
    	Row r1=new Row(s1);
    	n1.rowList.set(search_position, r1);
    	n1.intoBytes(thisTran);
    	
    	if(s1.get(0).contentEquals("master"))
    	{
            ByteArrayOutputStream byt=new ByteArrayOutputStream();
            ObjectOutputStream obj=new ObjectOutputStream(byt);
            
            Integer random;
            random=Page.pageCount;
            obj.writeObject(random);
            obj.writeObject(page);
            
            Page ppp=this.cacheManager.readPage(thisTran.tranNum, -2);
            ppp.getPageBuffer().rewind();
            ppp.getPageBuffer().put(byt.toByteArray());
            cacheManager.writePage(thisTran.tranNum,ppp);
    	}
    }
    
    public void split(Node root,Transaction thisTran) throws IOException, ClassNotFoundException
    {
    	int i,j;
    	Row media;
    	Node p1,p2;
    	p1=new Node(M,node_tablename,keyName,this.columnname,this.columntype,cacheManager,thisTran);
    	
    	for(i=M/2+1,j=0;i<root.rowList.size();i++,j++)
    	{
    		p1.rowList.add(root.rowList.get(i));
    		p1.sonNodeList.add(root.sonNodeList.get(i));
    		p2=new Node(root.sonNodeList.get(i),cacheManager,thisTran);
    		p2.fatherNodeID=p1.pageOne;
    		p2.intoBytes(thisTran);
    	}
    	p1.sonNodeList.add(root.sonNodeList.get(i));
    	p2=new Node(root.sonNodeList.get(i),cacheManager,thisTran);
		p2.fatherNodeID=p1.pageOne;
		p2.intoBytes(thisTran);
		
		p1.intoBytes(thisTran);
		
		
		for(;j>0;j--)
		{
			root.rowList.remove(root.rowList.size()-1);
			root.sonNodeList.remove(root.sonNodeList.size()-1);
		}
		root.sonNodeList.remove(root.sonNodeList.size()-1);
		
		media=root.rowList.get(root.rowList.size()-1);
		root.rowList.remove(root.rowList.size()-1);
		
		root.intoBytes(thisTran);
		
		if(root.fatherNodeID!=-1)
		{
			p1.fatherNodeID=root.fatherNodeID;
			p1.intoBytes(thisTran);
			
			i=0;
			p2=new Node(root.fatherNodeID,cacheManager,thisTran);
			while(!p2.sonNodeList.get(i).equals(root.pageOne))
			{
				i++;
			}
			
			p2.sonNodeList.add(-1);
			p2.rowList.add(media);
			for(j=p2.rowList.size()-1;j>i;j--)
			{
				p2.rowList.set(j, p2.rowList.get(j-1));
				p2.sonNodeList.set(j+1, p2.sonNodeList.get(j));
			}
			
			p2.rowList.set(j, media);
			p2.sonNodeList.set(j+1, p1.pageOne);
			p2.intoBytes(thisTran);
			
			if(p2.rowList.size()>M)
			{
				split(p2,thisTran);
			}
		}
		else {
			p2=new Node(M,node_tablename,keyName,this.columnname,this.columntype,cacheManager,thisTran);
			p1.fatherNodeID=p2.pageOne;
			root.fatherNodeID=p2.pageOne;
			
			p2.rowList.add(media);
			p2.sonNodeList.add(root.pageOne);
			p2.sonNodeList.add(p1.pageOne);
			
			p1.intoBytes(thisTran);
			p2.intoBytes(thisTran);
			root.intoBytes(thisTran);
			
			this.rhizine=p2;
			update_root(p2.pageOne,node_tablename,thisTran);
		}
    }
    
    
    //由于rhizine的影响，只允许用根所在的节点进行insert操作
    public boolean insertRow(Row row,Transaction thisTran) throws IOException, ClassNotFoundException {
        boolean insertOrNot = false;//row里的s1假设一共有2*n+1个元素，前n个元素是插入列具体值，后n个元素是插入列列名，最后一个元素是插入表的
                                    //主键的列名（只有一个名字，一个主键）
                                    //rowList的还是不会用这种方式储存，而是只是存key值  
        int insertNumber = 0;
        int key_position=0;
        
        for(key_position=0;key_position<this.columnname.size();key_position++)
        {
        	if(this.columnname.get(key_position).contentEquals(keyName))
        	{
        		break;
        	}
        }
        
        List<Row> rowList2;
        Node n1;
        rowList2=this.rhizine.rowList;
        n1=this.rhizine;
        while(n1.sonNodeList.size()!=0)
        {
        	int i;
        	for(i=0;i<rowList2.size();i++)
	        {
	        	if(rowList2.get(i).getCell(key_position).bigerThan(row.getCell(key_position)))
	        	{
	        		break;
	        	}
	        }
        	n1=new Node(n1.sonNodeList.get(i),cacheManager,thisTran);
        	rowList2=n1.rowList;
        }
        
        int i;
        
        int size=rowList2.size();
        
        rowList2.add(null);
        
        for(i=size-1;i>=0;i--)
        {
        	if(rowList2.get(i).getCell(key_position).bigerThan(row.getCell(key_position)))
        	{
        		rowList2.set(i+1,rowList2.get(i));
        	}
        	else {
        		break;
        	}
        }
        
        rowList2.set(i+1, row);
        n1.intoBytes(thisTran);
        
        
        if(rowList2.size()>M)
        {
        	Node n2=new Node(M,this.node_tablename,this.keyName,this.columnname,this.columntype,cacheManager,thisTran);
        	List<Row>rowList3=new ArrayList<Row>();
        	n2.rowList=rowList3;
        	
        	int j;
        	int size_local=rowList2.size();
        	for(i=M/2+1,j=0;i<size_local;i++,j++)
        	{
        		rowList3.add(rowList2.get(i));
        	}
        	n2.intoBytes(thisTran);
        	
        	Row media;
        	
        	for(;j>0;j--)
        	{
        		rowList2.remove(rowList2.size()-1);//
        	}
        	media=rowList2.get(rowList2.size()-1);
        	rowList2.remove(rowList2.size()-1);
        	n1.intoBytes(thisTran);
        	
        	if(n1.fatherNodeID!=-1)
        	{
        		n2.fatherNodeID=n1.fatherNodeID;
        		n2.intoBytes(thisTran);
        		
        		i=0;
        		Node n3;
        		n3=new Node(n1.fatherNodeID,cacheManager,thisTran);
        		while(!n3.sonNodeList.get(i).equals(n1.pageOne))
        		{
        			i++;
        		}
        		
        		n3.rowList.add(media);//随便加了一个元素为了使得n3的size加1
        		n3.sonNodeList.add(-1);//随便加了一个元素为了使得n3的size加1
        		
        		for(j=n3.sonNodeList.size()-2/*原来是减1但是上面size提前加了所以减2*/;j>i;j--)
        		{
        			n3.rowList.set(j,n3.rowList.get(j-1));
        			n3.sonNodeList.set(j+1, n3.sonNodeList.get(j));
        		}
        		
        		n3.rowList.set(i, media);
        		n3.sonNodeList.set(i+1, n2.pageOne);
        		n3.intoBytes(thisTran);
        		
        		if(n3.rowList.size()>M)
        		{
        			split(n3,thisTran);
        		}
        	}
        	else {
        		Node n3=new Node(M,this.node_tablename,this.keyName,this.columnname,this.columntype,cacheManager,thisTran);
        		n3.rowList.add(media);
        		n3.sonNodeList.add(n1.pageOne);
        		n3.sonNodeList.add(n2.pageOne);
        		n1.fatherNodeID=n3.pageOne;
        		n2.fatherNodeID=n3.pageOne;
        		n1.intoBytes(thisTran);
        		n2.intoBytes(thisTran);
        		n3.intoBytes(thisTran);
        		
        		this.rhizine=n3;
        		update_root(n3.pageOne,node_tablename,thisTran);
        	}
        }
        
        return true;
    }

    public boolean drop(Transaction thisTran)
    {
    	
        return true;
    }

    public Node get_brother_Node(Node root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node p1;
    	if(root.fatherNodeID!=-1)
    	{
    		Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    		for(i=0;i<root_father.sonNodeList.size();i++)
    		{
    			if(root_father.sonNodeList.get(i).equals(root.pageOne))
    			{
    				break;
    			}
    		}
    		
    		adjust_position=i;
    		
    		if(i>0)
    		{
    			p1=new Node(root_father.sonNodeList.get(i-1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=0;
	    			return p1;
	    		}
    		}
    		
    		if(i<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node(root_father.sonNodeList.get(i+1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=1;
	    			return p1;
	    		}
    		}
    		
    	}
    	return null;
    }
    
    public void adjust(Node root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i,j;
    	Node p1,p2;
    	p1=get_brother_Node(root,thisTran);
    	if(p1!=null)
    	{
    		if(left_or_right==0)
    		{
    			root.rowList.add(null);
    			
    			if(root.sonNodeList.size()!=0)
    			{
    				root.sonNodeList.add(null);
	    			for(i=root.rowList.size()-1;i>0;i--)
	    			{
	    				root.rowList.set(i, root.rowList.get(i-1));
	    				root.sonNodeList.set(i+1, root.sonNodeList.get(i));
	    			}
	    			root.sonNodeList.set(i+1, root.sonNodeList.get(i));
    			}
    			
    			else {
	    			for(i=root.rowList.size()-1;i>0;i--)
	    			{
	    				root.rowList.set(i, root.rowList.get(i-1));
	    			}
    			}
    			
    			Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.set(0, root_father.rowList.get(adjust_position-1));
    			
    			if(p1.sonNodeList.size()!=0)//叶子节点有儿子，不为空
    			{
    				root.sonNodeList.set(0, p1.sonNodeList.get(p1.sonNodeList.size()-1));
    				p2=new Node(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
    				p2.fatherNodeID=root.pageOne;
    				p1.sonNodeList.remove(p1.sonNodeList.size()-1);
    				p1.intoBytes(thisTran);
    				p2.intoBytes(thisTran);
    			}
    			
    			root_father.rowList.set(adjust_position-1, p1.rowList.get(p1.rowList.size()-1));
    			
    			p1.rowList.remove(p1.rowList.size()-1);
    			root.intoBytes(thisTran);
    			root_father.intoBytes(thisTran);
    			p1.intoBytes(thisTran);
    		}
    		else if(left_or_right==1)
    		{
    			Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.add(null);
    			root.rowList.set(root.rowList.size()-1, root_father.rowList.get(adjust_position));
    			if(p1.sonNodeList.size()!=0)
    			{
    				root.sonNodeList.add(null);
    				root.sonNodeList.set(root.sonNodeList.size()-1, p1.sonNodeList.get(0));
    				p2=new Node(p1.sonNodeList.get(0),cacheManager,thisTran);
    				p2.fatherNodeID=root.pageOne;
    				p2.intoBytes(thisTran);
    				//p1.sonNodeList.remove(p1.sonNodeList.size()-1);
    			}
    			
    			root_father.rowList.set(adjust_position, p1.rowList.get(0));
    			
    			
    			if(p1.sonNodeList.size()!=0)
    			{
    				for(i=0;i<p1.rowList.size()-1;i++)
	    			{
	    				p1.rowList.set(i, p1.rowList.get(i+1));
	    				p1.sonNodeList.set(i, p1.sonNodeList.get(i+1));
	    			}
	    			p1.sonNodeList.set(i, p1.sonNodeList.get(i+1));
    			}
    			
    			else {
    				for(i=0;i<p1.rowList.size()-1;i++)
	    			{
	    				p1.rowList.set(i, p1.rowList.get(i+1));
	    			}
    			}
    			
    			p1.rowList.remove(p1.rowList.size()-1);
    			if(p1.sonNodeList.size()!=0)
    			{
    				p1.sonNodeList.remove(p1.sonNodeList.size()-1);
    			}
    			p1.intoBytes(thisTran);
    			root.intoBytes(thisTran);
    			root_father.intoBytes(thisTran);
    		}
    	}
    	else if(p1==null&&root.fatherNodeID!=-1)
    	{
    		Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    		if(adjust_position>0)
    		{
    			p1=new Node(root_father.sonNodeList.get(adjust_position-1),cacheManager,thisTran);
    			p1.rowList.add(null);
    			p1.rowList.set(p1.rowList.size()-1, root_father.rowList.get(adjust_position-1));
    			for(i=adjust_position-1;i<root_father.rowList.size()-1;i++)
    			{
    				root_father.rowList.set(i, root_father.rowList.get(i+1));
    				root_father.sonNodeList.set(i+1, root_father.sonNodeList.get(i+2));
    			}
    			root_father.rowList.remove(root_father.rowList.size()-1);
    			root_father.sonNodeList.remove(root_father.sonNodeList.size()-1);
    			
    			for(j=0;j<root.rowList.size();j++)
    			{
    				p1.rowList.add(root.rowList.get(j));
    				if(root.sonNodeList.size()!=0)
    				{
    					p1.sonNodeList.add(root.sonNodeList.get(j));
    					p2=new Node(root.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=p1.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(root.sonNodeList.size()!=0)
				{
					p1.sonNodeList.add(root.sonNodeList.get(j));
					p2=new Node(root.sonNodeList.get(j),cacheManager,thisTran);
					p2.fatherNodeID=p1.pageOne;
					p2.intoBytes(thisTran);
				}
				p1.intoBytes(thisTran);
				root_father.intoBytes(thisTran);
				
				this.cacheManager.unusedList_PageID.add(root.pageOne);
				if(root_father.rowList.size()==0)
				{
					this.cacheManager.unusedList_PageID.add(root_father.pageOne);
					p1.fatherNodeID=-1;
					p1.intoBytes(thisTran);
					this.rhizine=p1;
					update_root(p1.pageOne, this.node_tablename, thisTran);
				}
				else if(root_father.rowList.size()<M/2)
				{
					adjust(root_father,thisTran);
				}
    		}
    		else if(adjust_position<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node(root_father.sonNodeList.get(adjust_position+1),cacheManager,thisTran);
    			root.rowList.add(null);
    			root.rowList.set(root.rowList.size()-1, root_father.rowList.get(adjust_position));
    			for(i=adjust_position;i<root_father.rowList.size()-1;i++)
    			{
    				root_father.rowList.set(i, root_father.rowList.get(i+1));
    				root_father.sonNodeList.set(i+1, root_father.sonNodeList.get(i+2));
    			}
    			root_father.rowList.remove(root_father.rowList.size()-1);
    			root_father.sonNodeList.remove(root_father.sonNodeList.size()-1);
    			
    			for(j=0;j<p1.rowList.size();j++)
    			{
    				root.rowList.add(p1.rowList.get(j));
    				if(p1.sonNodeList.size()!=0)
    				{
    					root.sonNodeList.add(p1.sonNodeList.get(j));
    					p2=new Node(p1.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=root.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(p1.sonNodeList.size()!=0)
				{
					root.sonNodeList.add(p1.sonNodeList.get(j));
					p2=new Node(p1.sonNodeList.get(j),cacheManager,thisTran);
					p2.fatherNodeID=root.pageOne;
					p2.intoBytes(thisTran);
				}
				
				root.intoBytes(thisTran);
				root_father.intoBytes(thisTran);
				
				this.cacheManager.unusedList_PageID.add(p1.pageOne);
				p1=null;
				if(root_father.rowList.size()==0)
				{
					this.cacheManager.unusedList_PageID.add(root_father.pageOne);
					root.fatherNodeID=-1;
					root.intoBytes(thisTran);
					this.rhizine=root;
					update_root(root.pageOne,this.node_tablename,thisTran);
				}
				else if(root_father.rowList.size()<M/2)
				{
					adjust(root_father,thisTran);
				}
    		}
    	}
    	else if(p1==null&&root.fatherNodeID==-1)
    	{
    		if(root.rowList.size()==0)
    		{
    			return ;
    		}
    	}
    }
    
    public boolean deleteRow(Cell key,int liehao,Transaction thisTran) throws IOException, ClassNotFoundException 
    {
        int i;
        int p1_position=0;
        Node p1,p2;
        p1=this.rhizine;
        while(true)
        {
        	for(i=0;i<p1.rowList.size();i++)
        	{
        		if(p1.rowList.get(i).getCell(liehao).bigerThan(key))
        		{
        			break;
        		}
        	}
        	if(i>0&&p1.rowList.get(i-1).getCell(liehao).equalTo(key))
        	{
        		p1_position=i-1;
        		break;
        	}
        	else {
        		if(i<p1.sonNodeList.size())
        		{
        			p1=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
        		}
        		else {
        			p1=null;
        			break;
        		}
        	}
        }
        
        if(p1!=null&&p1.sonNodeList.size()!=0)
        {
        	p2=new Node(p1.sonNodeList.get(p1_position),cacheManager,thisTran);
        	
        	while(p2.sonNodeList.size()!=0)
        	{
        		p2=new Node(p2.sonNodeList.get(p2.sonNodeList.size()-1),cacheManager,thisTran);
        	}
        	
        	p1.rowList.set(p1_position,p2.rowList.get(p2.rowList.size()-1));
        	p1.intoBytes(thisTran);
        	
        	
        	p2.rowList.remove(p2.rowList.size()-1);
        	p2.intoBytes(thisTran);
        	
        	if(p2.rowList.size()<M/2)
        	{
        		adjust(p2,thisTran);
        	}
        }
        else if(p1!=null&&p1.sonNodeList.size()==0) 
        {
        	for(i=p1_position;i<p1.rowList.size()-1;i++)////////set改了
        	{
        		p1.rowList.set(i, p1.rowList.get(i+1));
        	}
        	
        	p1.rowList.remove(p1.rowList.size()-1);
        	p1.intoBytes(thisTran);
        	
        	if(p1.rowList.size()<M/2)
        	{
        		adjust(p1,thisTran);
        	}
        	
        }
        
    	return true;
    }
    
    //只查找一个相等的元素就返回，没有相等的元素返回空
    public Node search_auto(int keyp,Cell key,Node root,Transaction thisTran) throws ClassNotFoundException, IOException//返回NOde在Node中的位置在search_position中
    {
    	int i;
    	Node p1;
    	p1=root;
    	while(true)
    	{
    		for(i=0;i<p1.rowList.size();i++)
    		{
    			if(p1.rowList.get(i).getCell(keyp).bigerThan(key))
    			{
    				break;
    			}
    		}
    		if(i>0&&p1.rowList.get(i-1).getCell(keyp).equalTo(key))
    		{
    			search_position=i-1;
    			break;
    		}
    		else {
    			if(p1.sonNodeList.size()!=0)
    			{
    				p1=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    			}
    			else {
    				p1=null;
    				break;
    			}
    		}
    	}
    	return p1;
    }
    
    public void search_all(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node p1;
    	this.stack_auto_top=0;
    	this.stack_auto=new Node[10000];
    	this.stack_auto[this.stack_auto_top]=root;
    	this.stack_auto_top++;
    	while(this.stack_auto_top!=0)
    	{
    		p1=this.stack_auto[this.stack_auto_top-1];
    		this.stack_auto_top--;
    		
    		if(p1!=null)
    		{
    			flag=0;
    			for(i=0;i<p1.rowList.size();i++)
    			{
    				if(p1.rowList.get(i).getCell(keyp).equalTo(key))
    				{
    					this.stack_auto[this.stack_auto_top]=new Node(p1.sonNodeList.get(i+1),cacheManager,thisTran);
    					this.stack_auto_top++;
    					if(flag==0)
    					{
    						flag=1;
    						this.stack_auto[this.stack_auto_top]=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    						this.stack_auto_top++;
    					}
    					/////对相等的key的节点的处理todo
    				}
    				else if(p1.rowList.get(i).getCell(keyp).bigerThan(key))
    				{
    					if(flag==0)
    					{
    						flag=2;
    						this.stack_auto[this.stack_auto_top]=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    						this.stack_auto_top++;
    					}
    					break;
    				}
    			}
    			if(flag==0)
    			{
    				this.stack_auto[this.stack_auto_top]=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    				this.stack_auto_top++;
    			}
    		}
    	}
    	this.stack_auto=null;
    }
    
    public void delete_all(Node root, Cell key, Integer keyp,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	
    	
    	this.counter=0;
    	count_key_number(root,key,keyp,thisTran);
    	for(i=0;i<this.counter;i++)
    	{
    		root.deleteRow(key,keyp,thisTran);
    	}
    }
    
/////////////////////////////////
    //count之前一定要置0
    ////////////////////////////////////////////
    //count之前一定要置0
    	////////////////////////////////////
    public void count_key_number(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {//count之前一定要置0
    	//count之前一定要置0
    	int i;
    	Node p1;
    	for(i=0;i<root.rowList.size();i++)
    	{
    		if(root.rowList.get(i).getCell(keyp).equalTo(key))
            {
            	counter++;
            }
    		else if(root.rowList.get(i).getCell(keyp).bigerThan(key))
    		{
    			break;
    		}
    	}
    	for(i=0;i<root.sonNodeList.size();i++)
    	{
    		p1=new Node(root.sonNodeList.get(i),cacheManager,thisTran);
    		count_key_number(p1,key,keyp,thisTran);
    	}
    }
    
    public void get_node_all(Node root, Transaction thisTran) throws ClassNotFoundException, IOException//得到节点的所有行和节点所有儿子的所有行
    {
    	int i;
    	Node p1,p2;
    	p1=root;
    	Stack<Node>s1=new Stack<Node>();
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		if(p1.sonNodeList.size()!=0)
    		{
    			p2=new Node(p1.sonNodeList.get(0),cacheManager,thisTran);
    			s1.push(p2);
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			cacheManager.search_result(p1.rowList.get(i));
	    			p2=new Node(p1.sonNodeList.get(i+1),cacheManager,thisTran);
	    			s1.push(p2);
	    		}
    		}
    		else {
    			for(i=0;i<p1.rowList.size();i++)
	    		{
	    			cacheManager.search_result(p1.rowList.get(i));
	    		}
    		}
    	}
    }
    
    public void search_greater_all(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException//不一定是主键了
    {
    	int i;
    	int flag=0;
    	Node p1,p2;
    	p1=root;
    	Stack<Node>s1=new Stack<Node>();
    	s1.push(p1);
    	while(!s1.empty())
		{
    		p1=s1.pop();
    		flag=0;
    		for(i=0;i<p1.rowList.size();i++)
			{
				if(p1.rowList.get(i).getCell(keyp).bigerThan(key))//第一个大于key的元素
				{
					flag=1;
					if(p1.sonNodeList.size()!=0)
					{
						p2=new Node(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
						s1.push(p2);
					}
					break;
				}
			}
			if(flag==1)
			{
				if(p1.sonNodeList.size()!=0)
				{
					for(;i<p1.rowList.size();i++)
					{
						cacheManager.search_result(p1.rowList.get(i));
						p2=new Node(p1.sonNodeList.get(i+1), cacheManager, thisTran);
						get_node_all(p2, thisTran);
					}
				}
				else {
					for(;i<p1.rowList.size();i++)
					{
						cacheManager.search_result(p1.rowList.get(i));
					}
				}
			}
			else if(flag==0)
			{
				if(p1.sonNodeList.size()!=0)
				{
					p2=new Node(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
					s1.push(p2);
				}
			}
		}
    	s1=null;
    }
    
    public void search_letter_all(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	p1=root;
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		flag=0;
    		for(i=p1.rowList.size()-1;i>=0;i--)
    		{
    			if(p1.rowList.get(i).getCell(keyp).letterThan(key))//第一个小于key的元素
    			{
    				flag=1;
    				if(p1.sonNodeList.size()!=0)
    				{
    					p2=new Node(p1.sonNodeList.get(i+1),cacheManager,thisTran);
    					s1.push(p2);
    				}
    				break;
    			}
    		}
    		
    		if(flag==1)
    		{
    			if(p1.sonNodeList.size()!=0)
    			{
    				for(;i>=0;i--)
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    				p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    					get_node_all(p2,thisTran);
	    			}
    			}
    			else {
    				for(;i>=0;i--)
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
    			}
    		}
    		else if(flag==0)
    		{
    			if(p1.sonNodeList.size()!=0)
    			{
    				p2=new Node(p1.sonNodeList.get(0),cacheManager,thisTran);
					s1.push(p2);
    			}
    		}
    	}
    	s1=null;
    }
    
    public void search_equal_all(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	p1=root;
    	
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		flag=0;
    		for(i=0;i<p1.rowList.size();i++)
    		{
    			if(p1.rowList.get(i).getCell(keyp).equalTo(key))//第一个等于key的元素
    			{
    				flag=1;
    				cacheManager.search_result(p1.rowList.get(i));
    				if(p1.sonNodeList.size()!=0)
    				{
    					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    					s1.push(p2);
    				}
    			}
    			else if(p1.rowList.get(i).getCell(keyp).bigerThan(key))
				{
    				flag=2;
    				if(p1.sonNodeList.size()!=0)
    				{
    					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    					s1.push(p2);
    				}
    				break;
				}
    		}
    		if(flag!=2)
    		{
				if(p1.sonNodeList.size()!=0)
				{
					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
				}
    		}
    	}
    	s1=null;
    }
    
    public void search_greater_or_equal_all(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	
    	p1=root;
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		flag=0;
    		for(i=0;i<p1.rowList.size();i++)
    		{
    			if(p1.rowList.get(i).getCell(keyp).bigerThan(key)||p1.rowList.get(i).getCell(keyp).equalTo(key))
    			{
    				flag=1;
    				if(p1.sonNodeList.size()!=0)
    				{
    					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
    					s1.push(p2);
    				}
    				break;
    			}
    		}
    		
    		if(flag==1)
    		{
    			if(p1.sonNodeList.size()!=0)
    			{
    				for(;i<p1.rowList.size();i++)
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    				p2=new Node(p1.sonNodeList.get(i+1),cacheManager,thisTran);
	    				get_node_all(p2,thisTran);
	    			}
    			}
    			else {
    				for(;i<p1.rowList.size();i++)
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
    			}
    		}
    		else if(flag==0)
    		{
    			if(p1.sonNodeList.size()!=0)
    			{
    				p2=new Node(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
    				s1.push(p2);
    			}
    		}
    	}
    	s1=null;
    }
    
    public void search_letter_or_equal_all(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	
    	p1=root;
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		flag=0;
    		for(i=p1.rowList.size()-1;i>=0;i--)
    		{
    			if(p1.rowList.get(i).getCell(keyp).letterThan(key)||p1.rowList.get(i).getCell(keyp).equalTo(key))
    			{
    				flag=1;
    				if(p1.sonNodeList.size()!=0)
    				{
    					p2=new Node(p1.sonNodeList.get(i+1),cacheManager,thisTran);
    					s1.push(p2);
    				}
    				break;
    			}
    		}
    		
    		if(flag==1)
    		{
    			if(p1.rowList.size()!=0)
    			{
    				for(;i>=0;i--)
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    				p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
	    				get_node_all(p2,thisTran);
	    			}
    			}
    			else {
    				for(;i>=0;i--)
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
    			}
    		}
    		else {
    			if(p1.sonNodeList.size()!=0)
    			{
    				p2=new Node(p1.sonNodeList.get(0),cacheManager,thisTran);
    				s1.push(p2);
    			}
    		}
    	}
    	s1=null;
    }

    
    ///////////////////////////////////////////////////////////////////
    //上面是按主键可以搜索的，下面是不可以按主键搜索的
    ///////////////////////////////////////////////////////////////////
    
    public void search_greater_all_nokey(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException//不一定是主键了
    {
    	int i;
    	Node p1,p2;
    	p1=root;
    	Stack<Node>s1=new Stack<Node>();
    	s1.push(p1);
    	while(!s1.empty())
		{
    		p1=s1.pop();
    		
    		if(p1.sonNodeList.size()!=0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
				{
					if(p1.rowList.get(i).getCell(keyp).bigerThan(key))//第一个大于key的元素
					{
						cacheManager.search_result(p1.rowList.get(i));
					}
					p2=new Node(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
					s1.push(p2);
				}
	    		p2=new Node(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
				s1.push(p2);
    		}
    		
    		else if(p1.sonNodeList.size()==0)
    		{
    			for(i=0;i<p1.rowList.size();i++)
				{
					if(p1.rowList.get(i).getCell(keyp).bigerThan(key))//第一个大于key的元素
					{
						cacheManager.search_result(p1.rowList.get(i));
					}
				}
    		}
		}
    	s1=null;
    }
    
    public void search_letter_all_nokey(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	p1=root;
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();

    		if(p1.sonNodeList.size()!=0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).letterThan(key))//第一个小于key的元素
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
	    			p2=new Node(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
					s1.push(p2);
	    		}
	    		p2=new Node(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
				s1.push(p2);
    		}
    		
    		else if(p1.sonNodeList.size()==0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).letterThan(key))//第一个小于key的元素
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
	    		}
    		}
    		
    	}
    	s1=null;
    }
    
    public void search_equal_all_nokey(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	p1=root;
    	
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		if(p1.sonNodeList.size()!=0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).equalTo(key))//第一个等于key的元素
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
	    		}
				p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
				s1.push(p2);
    		}
    		
    		else if(p1.sonNodeList.size()==0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).equalTo(key))//第一个等于key的元素
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
	    		}
    		}
    		
    	}
    	s1=null;
    }
    
    public void search_greater_or_equal_all_nokey(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	
    	p1=root;
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		if(p1.sonNodeList.size()!=0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).bigerThan(key)||p1.rowList.get(i).getCell(keyp).equalTo(key))
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
	    		}
				p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
				s1.push(p2);
    		}
    		
    		else if(p1.sonNodeList.size()==0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).bigerThan(key)||p1.rowList.get(i).getCell(keyp).equalTo(key))
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
	    		}
    		}
    		
    	}
    	s1=null;
    }
    
    public void search_letter_or_equal_all_nokey(Node root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node p1,p2;
    	Stack<Node>s1=new Stack<Node>();
    	
    	p1=root;
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		
    		if(p1.sonNodeList.size()!=0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).letterThan(key)||p1.rowList.get(i).getCell(keyp).equalTo(key))
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
					p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
	    		}
				p2=new Node(p1.sonNodeList.get(i),cacheManager,thisTran);
				s1.push(p2);
    		}
    		
    		else if(p1.sonNodeList.size()==0)
    		{
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			if(p1.rowList.get(i).getCell(keyp).letterThan(key)||p1.rowList.get(i).getCell(keyp).equalTo(key))
	    			{
	    				cacheManager.search_result(p1.rowList.get(i));
	    			}
	    		}
    		}
    		
    	}
    	s1=null;
    }
    
    public Node get_brother_Node_nokey(Node root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node p1;
    	if(root.fatherNodeID!=-1)
    	{
    		Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    		for(i=0;i<root_father.sonNodeList.size();i++)
    		{
    			if(root_father.sonNodeList.get(i).equals(root.pageOne))
    			{
    				break;
    			}
    		}
    		
    		adjust_position=i;
    		
    		if(i>0)
    		{
	    		p1=new Node(root_father.sonNodeList.get(i-1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=0;
	    			return p1;
	    		}
    		}
    		
    		if(i<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node(root_father.sonNodeList.get(i+1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=1;
	    			return p1;
	    		}	
    		}
    	}
    	return null;
    }
    
    public void adjust_nokey(Node root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i,j;
    	Node p1,p2;
    	p1=get_brother_Node_nokey(root,thisTran);
    	if(p1!=null)
    	{
    		if(left_or_right==0)
    		{
    			root.rowList.add(null);
    			if(root.sonNodeList.size()!=0)
    			{
    				root.sonNodeList.add(null);
	    			for(i=root.rowList.size()-1;i>0;i--)
	    			{
	    				root.rowList.set(i, root.rowList.get(i-1));
	    				root.sonNodeList.set(i+1, root.sonNodeList.get(i));
	    			}
	    			root.sonNodeList.set(i+1, root.sonNodeList.get(i));
    			}
    			
    			else {
    				for(i=root.rowList.size()-1;i>0;i--)
	    			{
	    				root.rowList.set(i, root.rowList.get(i-1));
	    			}
    			}
    			
    			Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.set(0, root_father.rowList.get(adjust_position-1));
    			
    			if(p1.sonNodeList.size()!=0)//叶子节点有儿子，不为空
    			{
    				root.sonNodeList.set(0, p1.sonNodeList.get(p1.sonNodeList.size()-1));
    				p2=new Node(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
    				p2.fatherNodeID=root.pageOne;
    				p1.sonNodeList.remove(p1.sonNodeList.size()-1);
    				p1.intoBytes(thisTran);
    				p2.intoBytes(thisTran);
    			}
    			
    			root_father.rowList.set(adjust_position-1, p1.rowList.get(p1.rowList.size()-1));
    			
    			p1.rowList.remove(p1.rowList.size()-1);
    			root.intoBytes(thisTran);
    			root_father.intoBytes(thisTran);
    			p1.intoBytes(thisTran);
    		}
    		else if(left_or_right==1)
    		{
    			Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.add(null);
    			root.rowList.set(root.rowList.size()-1, root_father.rowList.get(adjust_position));
    			if(p1.sonNodeList.size()!=0)
    			{
    				root.sonNodeList.add(null);
    				root.sonNodeList.set(root.sonNodeList.size()-1, p1.sonNodeList.get(0));
    				p2=new Node(p1.sonNodeList.get(0),cacheManager,thisTran);
    				p2.fatherNodeID=root.pageOne;
    				p2.intoBytes(thisTran);
    				//p1.sonNodeList.remove(p1.sonNodeList.size()-1);
    			}
    			
    			root_father.rowList.set(adjust_position, p1.rowList.get(0));
    			
    			if(p1.sonNodeList.size()!=0)
    			{
    				for(i=0;i<p1.rowList.size()-1;i++)
	    			{
	    				p1.rowList.set(i, p1.rowList.get(i+1));
	    				p1.sonNodeList.set(i, p1.sonNodeList.get(i+1));
	    			}
	    			p1.sonNodeList.set(i, p1.sonNodeList.get(i+1));
    			}
    			
    			else {
    				for(i=0;i<p1.rowList.size()-1;i++)
	    			{
	    				p1.rowList.set(i, p1.rowList.get(i+1));
	    			}
    			}
    			
    			p1.rowList.remove(p1.rowList.size()-1);
    			
    			if(p1.sonNodeList.size()!=0)
    			{
    				p1.sonNodeList.remove(p1.sonNodeList.size()-1);
    			}
    			p1.intoBytes(thisTran);
    			root.intoBytes(thisTran);
    			root_father.intoBytes(thisTran);
    		}
    	}
    	else if(p1==null&&root.fatherNodeID!=-1)
    	{
    		Node root_father=new Node(root.fatherNodeID,cacheManager,thisTran);
    		if(adjust_position>0)
    		{
    			p1=new Node(root_father.sonNodeList.get(adjust_position-1),cacheManager,thisTran);
    			p1.rowList.add(null);
    			p1.rowList.set(p1.rowList.size()-1, root_father.rowList.get(adjust_position-1));
    			for(i=adjust_position-1;i<root_father.rowList.size()-1;i++)
    			{
    				root_father.rowList.set(i, root_father.rowList.get(i+1));
    				root_father.sonNodeList.set(i+1, root_father.sonNodeList.get(i+2));
    			}
    			root_father.rowList.remove(root_father.rowList.size()-1);
    			root_father.sonNodeList.remove(root_father.sonNodeList.size()-1);
    			
    			for(j=0;j<root.rowList.size();j++)
    			{
    				p1.rowList.add(root.rowList.get(j));
    				if(root.sonNodeList.size()!=0)
    				{
    					p1.sonNodeList.add(root.sonNodeList.get(j));
    					p2=new Node(root.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=p1.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(root.sonNodeList.size()!=0)
				{
					p1.sonNodeList.add(root.sonNodeList.get(j));
					p2=new Node(root.sonNodeList.get(j),cacheManager,thisTran);
					p2.fatherNodeID=p1.pageOne;
					p2.intoBytes(thisTran);
				}
				p1.intoBytes(thisTran);
				root_father.intoBytes(thisTran);
				
				this.cacheManager.unusedList_PageID.add(root.pageOne);
				if(root_father.rowList.size()==0)
				{
					this.cacheManager.unusedList_PageID.add(root_father.pageOne);
					p1.fatherNodeID=-1;
					p1.intoBytes(thisTran);
					this.rhizine=p1;
					update_root(p1.pageOne, this.node_tablename, thisTran);
				}
				else if(root_father.rowList.size()<M/2)
				{
					adjust_nokey(root_father,thisTran);
				}
    		}
    		else if(adjust_position<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node(root_father.sonNodeList.get(adjust_position+1),cacheManager,thisTran);
    			root.rowList.add(null);
    			root.rowList.set(root.rowList.size()-1, root_father.rowList.get(adjust_position));
    			for(i=adjust_position;i<root_father.rowList.size()-1;i++)
    			{
    				root_father.rowList.set(i, root_father.rowList.get(i+1));
    				root_father.sonNodeList.set(i+1, root_father.sonNodeList.get(i+2));
    			}
    			root_father.rowList.remove(root_father.rowList.size()-1);
    			root_father.sonNodeList.remove(root_father.sonNodeList.size()-1);
    			
    			for(j=0;j<p1.rowList.size();j++)
    			{
    				root.rowList.add(p1.rowList.get(j));
    				if(p1.sonNodeList.size()!=0)
    				{
    					root.sonNodeList.add(p1.sonNodeList.get(j));
    					p2=new Node(p1.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=root.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(p1.sonNodeList.size()!=0)
				{
					root.sonNodeList.add(p1.sonNodeList.get(j));
					p2=new Node(p1.sonNodeList.get(j),cacheManager,thisTran);
					p2.fatherNodeID=root.pageOne;
					p2.intoBytes(thisTran);
				}
				
				root.intoBytes(thisTran);
				
				root_father.intoBytes(thisTran);
				
				this.cacheManager.unusedList_PageID.add(p1.pageOne);
				p1=null;
				if(root_father.rowList.size()==0)
				{
					this.cacheManager.unusedList_PageID.add(root_father.pageOne);
					root.fatherNodeID=-1;
					root.intoBytes(thisTran);
					this.rhizine=p1;
					update_root(root.pageOne,this.node_tablename,thisTran);
				}
				else if(root_father.rowList.size()<M/2)
				{
					adjust_nokey(root_father,thisTran);
				}
    		}
    	}
    	else if(p1==null&&root.fatherNodeID==-1)
    	{
    		if(root.rowList.size()==0)
    		{
    			return ;
    		}
    	}
    }
    
    public boolean deleteRow_nokey(Node root,Integer p1_position_arg,Transaction thisTran) throws IOException, ClassNotFoundException {
        int i;
        int p1_position=0;
        Node p1,p2;
        
        p1=root;
        
        p1_position=p1_position_arg;
        
        if(p1!=null&&p1.sonNodeList.size()!=0)
        {
        	p2=new Node(p1.sonNodeList.get(p1_position),cacheManager,thisTran);
        	
        	while(p2.sonNodeList.size()!=0)
        	{
        		p2=new Node(p2.sonNodeList.get(p2.sonNodeList.size()-1),cacheManager,thisTran);
        	}
        	
        	p1.rowList.set(p1_position,p2.rowList.get(p2.rowList.size()-1));
        	p1.intoBytes(thisTran);
        	
        	
        	p2.rowList.remove(p2.rowList.size()-1);
        	p2.intoBytes(thisTran);
        	
        	if(p2.rowList.size()<M/2)
        	{
        		adjust_nokey(p2,thisTran);
        	}
        }
        else if(p1!=null&&p1.sonNodeList.size()==0) 
        {
        	for(i=p1_position;i<p1.rowList.size()-1;i++)
        	{
        		p1.rowList.set(i, p1.rowList.get(i+1));
        	}
        	
        	p1.rowList.remove(p1.rowList.size()-1);
        	p1.intoBytes(thisTran);
        	
        	if(p1.rowList.size()<M/2)
        	{
        		adjust_nokey(p1,thisTran);
        	}
        	
        }
        
    	return true;
    }
    
    public void deleteRow_nokey_pre(Node root, List<Cell> keys, List<Integer> keyps, List<String> condition, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i,j;
    	int flag;
    	Node p1;
    	Stack<Node> st1=new Stack<Node>();
    	st1.push(root);
    	while(!st1.empty())
    	{
    		p1=st1.pop();
    		if(p1.sonNodeList.size()!=0)
    		{
    			for(i=0;i<p1.rowList.size();i++)
	        	{
    				flag=1;
	    			for(j=0;j<keyps.size();j++)
	    			{
	    				switch(condition.get(j))
	    				{
	    					case "EQ":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
	    							flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j))&&!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j)))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j))))&&!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
		        		}
	    			}
	    			if(flag==1)
	    			{
	    				deleteRow_nokey(p1,i,thisTran);
	                	return ;
	    			}
	    			st1.push(new Node(p1.sonNodeList.get(i),cacheManager,thisTran));
	        	}
    			st1.push(new Node(p1.sonNodeList.get(i),cacheManager,thisTran));
    		}
    		else {
    			for(i=0;i<p1.rowList.size();i++)
	        	{
    				flag=1;
	    			for(j=0;j<keyps.size();j++)
	    			{
	    				switch(condition.get(j))
	    				{
	    					case "EQ":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
	    							flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j))&&!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j)))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j))))&&!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
		        		}
	    			}
	    			if(flag==1)
	    			{
	    				deleteRow_nokey(p1,i,thisTran);
	                	return ;
	    			}
	        	}
    		}
    	}
    }
    
    public void count_key_number_nokey(Node root, List<Cell> keys, List<Integer> keyps, List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
    {//count之前一定要置0
    	//count之前一定要置0
    	int i,j;
    	int flag;
    	Node p1;
    	Stack<Node> st1=new Stack<Node>();
    	st1.push(root);
    	while(!st1.empty())
    	{
    		p1=st1.pop();
    		if(p1.sonNodeList.size()!=0)
    		{
    			for(i=0;i<p1.rowList.size();i++)
	        	{
    				flag=1;
	    			for(j=0;j<keyps.size();j++)
	    			{
	    				switch(condition.get(j))
	    				{
	    					case "EQ":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
	    							flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j))&&!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j)))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j))))&&!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
		        		}
	    			}
	    			if(flag==1)
	    			{
	    				counter++;
	    			}
	    			st1.push(new Node(p1.sonNodeList.get(i),cacheManager,thisTran));
	        	}
    			st1.push(new Node(p1.sonNodeList.get(i),cacheManager,thisTran));
    		}
    		else {
    			for(i=0;i<p1.rowList.size();i++)
	        	{
    				flag=1;
	    			for(j=0;j<keyps.size();j++)
	    			{
	    				switch(condition.get(j))
	    				{
	    					case "EQ":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
	    							flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j))&&!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j)))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j))))&&!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
		        		}
	    			}
	    			if(flag==1)
	    			{
	    				counter++;
	    			}
	        	}
    		}
    	}
    }
    
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    public void delete_all_nokey(Node root, List<Cell> keys, List<Integer> keyps,List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;

    	this.counter=0;

    	count_key_number_nokey(root,keys,keyps,condition,thisTran);
    	
    	this.rhizine=root;
    	
    	for(i=0;i<this.counter;i++)
    	{
    		root.deleteRow_nokey_pre(this.rhizine,keys,keyps, condition,thisTran);
    	}
    }
    
    
    public void count_key_number_nokey2(Node root, List<Cell> keys, List<Integer> keyps, List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
    {//count之前一定要置0
    	//count之前一定要置0
    	int i,j;
    	int flag;
    	Node p1;
    	Stack<Node> st1=new Stack<Node>();
    	st1.push(root);
    	while(!st1.empty())
    	{
    		p1=st1.pop();
    		if(p1.sonNodeList.size()!=0)
    		{
    			for(i=0;i<p1.rowList.size();i++)
	        	{
    				flag=1;
	    			for(j=0;j<keyps.size();j++)
	    			{
	    				switch(condition.get(j))
	    				{
	    					case "EQ":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
	    							flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j))&&!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j)))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j))))&&!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
		        		}
	    			}
	    			if(flag==1)
	    			{
	    				counter++;
	    				this.cacheManager.delete_update.add(p1.rowList.get(i));
	    			}
	    			st1.push(new Node(p1.sonNodeList.get(i),cacheManager,thisTran));
	        	}
    			st1.push(new Node(p1.sonNodeList.get(i),cacheManager,thisTran));
    		}
    		else {
    			for(i=0;i<p1.rowList.size();i++)
	        	{
    				flag=1;
	    			for(j=0;j<keyps.size();j++)
	    			{
	    				switch(condition.get(j))
	    				{
	    					case "EQ":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
	    							flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "GE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j))&&!p1.rowList.get(i).getCell(keyps.get(j)).bigerThan((keys.get(j))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LT":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j)))))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
	    					
	    					case "LE":
	    					{
	    						if(!p1.rowList.get(i).getCell(keyps.get(j)).letterThan(((keys.get(j))))&&!p1.rowList.get(i).getCell(keyps.get(j)).equalTo(keys.get(j)))
    			                {
    			                	flag=0;
    			                }
	    						break;
	    					}
		        		}
	    			}
	    			if(flag==1)
	    			{
	    				counter++;
	    				this.cacheManager.delete_update.add(p1.rowList.get(i));
	    			}
	        	}
    		}
    	}
    }
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    //这个函数传入的节点一定是根节点，否则会出现rhizine的错误
    public void delete_all_nokey2(Node root, List<Cell> keys, List<Integer> keyps, List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;

    	this.counter=0;

    	count_key_number_nokey2(root,keys,keyps, condition,thisTran);
    	
    	this.rhizine=root;
    	
    	for(i=0;i<this.counter;i++)
    	{
    		root.deleteRow_nokey_pre(this.rhizine,keys,keyps,condition,thisTran);
    	}
    }
    
    public void delete_zheng_ge_shu_bao_cun_geng_ye(Node root,Transaction thisTran) throws ClassNotFoundException, IOException//也保存到内存的更新表中
    {
    	int i;
    	Node n1;
    	Stack<Node>st1=new Stack<Node>();
    	n1=root;
    	
    	for(i=0;i<n1.sonNodeList.size();i++)
    	{
    		st1.push(new Node(n1.sonNodeList.get(i),this.cacheManager,thisTran));
    	}
    	
    	for(i=0;i<n1.rowList.size();i++)
    	{
    		this.cacheManager.delete_update.add(n1.rowList.get(i));
    	}
    	
    	n1.sonNodeList.clear();
    	n1.rowList.clear();
    	n1.fatherNodeID=-1;
    	n1.rhizine=n1;
    	n1.intoBytes(thisTran);
    	
    	while(!st1.empty())
    	{
    		n1=st1.pop();
    		if(n1.sonNodeList.size()!=0)
        	{
    			for(i=0;i<n1.rowList.size();i++)
	        	{
    				this.cacheManager.delete_update.add(n1.rowList.get(i));
	        		st1.push(new Node(n1.sonNodeList.get(i),this.cacheManager,thisTran));
	        	}
    			st1.push(new Node(n1.sonNodeList.get(i),this.cacheManager,thisTran));
    			this.cacheManager.unusedList_PageID.add(n1.pageOne);
        	}
    		else {
    			for(i=0;i<n1.rowList.size();i++)
	        	{
    				this.cacheManager.delete_update.add(n1.rowList.get(i));
	        	}
    			this.cacheManager.unusedList_PageID.add(n1.pageOne);
    		}
    	}
    }
    
    
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
//    private Node(List<Row> thisRowList, List<Integer> thisSonList, int thisOrder, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
//        ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
//        rowList = thisRowList;
//        sonNodeList = thisSonList;
//        fatherNodeID = -1;
//        this.cacheManager = cacheManager;
//        pageOne = new Page(buffer);
//        for (int i = 0; i < sonNodeList.size(); i++)
//        {
//            Node sonNode = new Node(sonNodeList.get(i),cacheManager,thisTran);
//            sonNode.setFather(pageOne.getPageID(),thisTran);
//            sonNode.setOrder(i,thisTran);
//        }
//        order = thisOrder;
//        intoBytes(thisTran);
//
//    }
//    
//    private Node(List<Row> thisRowList, List<Integer> thisSonList, int thisOrder, int father, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
//        ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
//        rowList = thisRowList;
//        sonNodeList = thisSonList;
//        fatherNodeID = father;
//        this.cacheManager = cacheManager;
//        pageOne = new Page(buffer);
//        for (int i = 0; i < sonNodeList.size(); i++)
//        {
//            Node sonNode = new Node(sonNodeList.get(i),cacheManager,thisTran);
//            sonNode.setFather(pageOne.getPageID(),thisTran);
//            sonNode.setOrder(i,thisTran);
//        }
//        order = thisOrder;
//        intoBytes(thisTran);
//    }
    
    
    
    
    
    
    private boolean setFather(int ID,Transaction thisTran) throws IOException {
        fatherNodeID = ID;

        // 缁存姢page淇℃伅
        intoBytes(thisTran);

        return true;
    }


    private boolean setOrder(int or, Transaction thisTran) throws IOException {
        order = or;

        // 缁存姢page淇℃伅
        intoBytes(thisTran);

        return true;
    }

    // 鍒嗚闄ゆ牴鑺傜偣澶栫殑鍏朵粬鑺傜偣銆
//    private Node devideNode(Transaction thisTran) throws IOException, ClassNotFoundException {
//        List<Row> rightRow;
//        List<Integer> rightNode;
//        rightRow = subRowList(rowList,M/2 + 1, M-1);
//        rowList = subRowList(rowList,0, M/2);
//        if(sonNodeList.size() == 0)
//        {
//            rightNode = new ArrayList<Integer>();
//        }
//        else
//        {
//            rightNode = subNodeList(sonNodeList,M/2 + 1,M);
//            sonNodeList = subNodeList(sonNodeList,0, M/2 + 1);
//        }
//
//        //缁存姢鏈琾age淇℃伅
//        intoBytes(thisTran);
//
//        return new Node(rightRow, rightNode, order + 1, cacheManager, thisTran);
//    }

    // 鍒嗚鏍硅妭鐐
//    private boolean rootDevideNode(Transaction thisTran) throws IOException, ClassNotFoundException {
//        List<Row> leftRow;
//        List<Row> rightRow;
//        List<Integer> leftNode;
//        List<Integer> rightNode;
//        leftRow = subRowList(rowList,0,M/2-1);
//        if(sonNodeList.size() == 0)
//        {
//            leftNode = new ArrayList<Integer>();
//        }
//        else
//        {
//            leftNode = subNodeList(sonNodeList,0,M/2);
//        }
//        rightRow = subRowList(rowList,M/2 + 1, M-1);
//        if(sonNodeList.size() == 0)
//        {
//            rightNode = new ArrayList<Integer>();
//        }
//        else
//        {
//            rightNode =subNodeList(sonNodeList,M/2 + 1, M);
//        }
//        rowList = subRowList(rowList,M/2, M/2);
//        List<Integer> newSonList = new ArrayList<Integer>();
//        newSonList.add(new Node(leftRow, leftNode, 0, pageOne.getPageID() ,cacheManager, thisTran).pageOne.getPageID());
//        newSonList.add(new Node(rightRow, rightNode,1,pageOne.getPageID() , cacheManager, thisTran).pageOne.getPageID());
//        sonNodeList = newSonList;
//
//        //缁存姢鏈琾age淇℃伅
//        intoBytes(thisTran);
//
//        return true;
//    }

    private List<Integer> subNodeList(List<Integer>list, int a, int b)
    {
        List<Integer> ret = new ArrayList<Integer>();
        for(int i = a; i <= b; i++)
        {
            ret.add(list.get(i));
        }
        return ret;
    }

    private List<Row> subRowList(List<Row>list, int a, int b)
    {
        List<Row> ret = new ArrayList<Row>();
        for(int i = a; i <= b; i++)
        {
            ret.add(list.get(i));
        }
        return ret;
    }

    // 璋冩暣鏈妭鐐逛娇鍏堕『搴忎负sonOrder鐨勫効瀛恟ow鏁伴噺鎭㈠鑷矼/2
//    private boolean adjustNode(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {
//        Node thisSonNode = new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);
//
//        // 鎺掗櫎鏈�澶у�艰竟鐣岃秺鐣屾儏鍐碉紝鍚戝乏涓嬪悎骞�
//        if (sonOrder < sonNodeList.size() - 1)
//        {
//            Node rightSonNode = new Node(sonNodeList.get(sonOrder + 1), cacheManager, thisTran);
//            if (rightSonNode.rowList.size() > M/2)
//            {
//                thisSonNode.insertRow(rowList.get(sonOrder),thisTran);
//                rightSonNode.deleteRow(rowList.get(order).getCell(0),thisTran);
//                rowList.set(sonOrder, rightSonNode.getFirstRow(thisTran));
//                //缁存姢鏈琾age淇℃伅
//                intoBytes(thisTran);
//                return true;
//            }
//        }
//
//        // 鎺掗櫎闆跺�艰竟鐣岃秺鐣屾儏鍐碉紝鍚戝彸涓嬪悎骞�
//        if (sonOrder > 0)
//        {
//            Node leftSonNode = new Node(sonNodeList.get(sonOrder - 1),cacheManager,thisTran);
//            if (leftSonNode.rowList.size() > M/2)
//            {
//                thisSonNode.insertRow(rowList.get(sonOrder - 1),thisTran);
//                leftSonNode.deleteRow(rowList.get(order).getCell(0),thisTran);
//                rowList.set(sonOrder - 1, leftSonNode.getLastRow(thisTran));
//                //缁存姢鏈琾age淇℃伅
//                intoBytes(thisTran);
//                return true;
//            }
//        }
//
//        // 娌℃湁鐩搁偦鐨勫彲鏀彺鍏勫紵鑺傜偣锛屽彧濂藉垹闄ゆ鑺傜偣銆
//        return deleteNode(sonOrder,thisTran);
//    }

    // 鍦ㄦ湰鑺傜偣涓坊鍔犲瓙鑺傜偣锛屽垎鍒坊鍔爎ow鍜屽搴旂殑SonNode銆
//    private boolean addNode(Row row, Node node, Transaction thisTran) throws IOException, ClassNotFoundException {
//        // 鐢ㄤ簬璁板綍鏄惁娣诲姞浜嗚繖涓妭鐐广��
//        boolean addOrNot = false;
//        for (int i = 0; i < rowList.size(); i++)
//        {
//            Row thisRow = rowList.get(i);
//            Node thisNode = new Node(sonNodeList.get(i),cacheManager,thisTran);
//            if (!addOrNot && thisRow.getCell(0).bigerThan(row.getCell(0)))
//            {
//                rowList.add(i, row);
//                sonNodeList.add(i, node.pageOne.getPageID());
//                // 缁存姢page淇℃伅銆�
//                intoBytes(thisTran);
//                thisNode.setFather(pageOne.getPageID(),thisTran);
//                addOrNot = true;
//            }
//            thisNode.setOrder(i,thisTran);
//        }
//        // 濡傛灉涔嬪墠閮芥病鏈夋坊鍔犺繖涓妭鐐癸紝閭ｄ箞姝ゆ椂娣诲姞鑷虫湯灏俱
//        if (!addOrNot)
//        {
//            rowList.add(row);
//            sonNodeList.add(sonNodeList.size() - 2, node.pageOne.getPageID());
//            // 缁存姢page淇℃伅銆�
//            intoBytes(thisTran);
//            node.setOrder(sonNodeList.size() - 1,thisTran);
//            node.setFather(pageOne.getPageID(),thisTran);
//        }
//
//        // 褰撴湭瓒呭嚭闀垮害鏃讹紝鎻掑叆瀹屾瘯銆
//        if (rowList.size() <= M)
//        {
//            return true;
//        }
//        // 瓒呭嚭闀垮害鏃讹紝杩涜鍗曞厓鍒嗚銆
//        else
//        {
//            if (fatherNodeID < 0)
//            {
//                return rootDevideNode(thisTran);
//            }
//            else
//            {
//                Node fatherNode = new Node(fatherNodeID,cacheManager,thisTran);
//                Node nodeTwo = devideNode(thisTran);
//                return fatherNode.addNode(rowList.get(M/2),nodeTwo,thisTran);
//            }
//        }
//    }

//    private boolean deleteNode(int sonOrder,Transaction thisTran) throws IOException, ClassNotFoundException {
//        Row thisRow;
//        if (sonOrder < sonNodeList.size() - 1)
//        {
//            thisRow = rowList.get(sonOrder);
//            Node rightNode = new Node(sonNodeList.get(sonOrder + 1),cacheManager,thisTran);
//            rightNode.insertRow(thisRow,thisTran);
//            rowList.remove(sonOrder);
//            sonNodeList.remove(sonOrder);
//            // 缁存姢page淇℃伅
//            intoBytes(thisTran);
//            for (int i = sonOrder; i < sonNodeList.size(); i++)
//            {
//                new Node(sonNodeList.get(i),cacheManager,thisTran).setOrder(i,thisTran);
//            }
//            if (rowList.size() < M/2)
//            {
//                if (fatherNodeID < 0)
//                {
//                    if (rowList.size() < 1)
//                    {
//                        rowList = new Node(sonNodeList.get(0),cacheManager,thisTran).rowList;
//                        sonNodeList = new Node(sonNodeList.get(0),cacheManager,thisTran).sonNodeList;
//                        // 缁存姢page淇℃伅
//                        intoBytes(thisTran);
//                        return true;
//                    }
//                    else
//                    {
//                        return true;
//                    }
//                }
//                else
//                {
//                    return new Node(fatherNodeID,cacheManager,thisTran).adjustNode(order,thisTran);
//                }
//            }
//            else
//            {
//                return true;
//            }
//        }
//        else
//        {
//            thisRow = rowList.get(sonOrder - 1);
//            Node leftNode = new Node(sonNodeList.get(sonOrder - 1),cacheManager,thisTran);
//            leftNode.insertRow(thisRow,thisTran);
//
//            rowList.remove(sonOrder - 1);
//            sonNodeList.remove(sonOrder);
//            // 缁存姢page淇℃伅
//            intoBytes(thisTran);
//            for (int i = sonOrder; i < sonNodeList.size(); i++)
//            {
//                new Node(sonNodeList.get(i),cacheManager,thisTran).setOrder(i,thisTran);
//            }
//            if (rowList.size() < M/2)
//            {
//                if (fatherNodeID < 0)
//                {
//                    if (rowList.size() < 1)
//                    {
//                        rowList = new Node(sonNodeList.get(0),cacheManager,thisTran).rowList;
//                        sonNodeList = new Node(sonNodeList.get(0),cacheManager,thisTran).sonNodeList;
//                        // 缁存姢page淇℃伅
//                        intoBytes(thisTran);
//                        return true;
//                    }
//                    else
//                    {
//                        return true;
//                    }
//                }
//                else
//                {
//                    return new Node(fatherNodeID,cacheManager,thisTran).adjustNode(order,thisTran);
//                }
//            }
//            else
//            {
//                return true;
//            }
//        }
//    }

    protected List<Integer> getSonNodeList()
    {
        return sonNodeList;

    }

    protected int getFatherNodeID()
    {
        return fatherNodeID;
    }

    protected Node getSpecialSonNode(int sonOrder,Transaction thisTran) throws IOException, ClassNotFoundException {
        return new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);
    }

    protected List<Row> getRowList()
    {
        return rowList;
    }
    protected Node getFatherNode(Transaction thisTran) throws IOException, ClassNotFoundException
    {
        return new Node(fatherNodeID,cacheManager,thisTran);
    }
    protected int getOrder()
    {
        return order;
    }
    
    
    
    
    public Row getRow(int id)
    {
        if (rowList.size() > 0)
        {
            return rowList.get(id);
        }
        else
        {
            return null;
        }
    }

    public Row getFirstRow(Transaction thisTran) throws IOException, ClassNotFoundException {
    	if((rowList == null) || (rowList.size() == 0)) {
    		return null;
    	}
    	
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return rowList.get(0);
        }
        else
        {
            return new Node(sonNodeList.get(0),cacheManager,thisTran).getFirstRow(thisTran);
        }
    }

    public Row getLastRow(Transaction thisTran) throws IOException, ClassNotFoundException {
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return rowList.get(rowList.size() - 1);
        }
        else
        {
            return new Node(sonNodeList.get(sonNodeList.size() - 1),cacheManager,thisTran).getLastRow(thisTran);
        }
    }

    public Row getSpecifyRow(Cell key,Transaction thisTran) throws IOException, ClassNotFoundException {
        int insertNumber = -1;
        for (int i = 0; i < rowList.size(); i++)
        {
            Row thisRow = rowList.get(i);
            if (thisRow.getCell(0).equalTo(key))
            {
                return thisRow;
            }
            else if (thisRow.getCell(0).bigerThan(key))
            {
                if ((sonNodeList == null) || (sonNodeList.size() == 0))
                {
                    insertNumber = i;
                    break;
                }
                else
                {
                    return new Node(sonNodeList.get(i),cacheManager,thisTran).getSpecifyRow(key,thisTran);
                }
            }
        }
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            if (insertNumber > 0)
            {
                return rowList.get(insertNumber);
            }
            else
            {
                return null;
            }
        }
        else
        {
            return new Node(sonNodeList.get(sonNodeList.size() - 1),cacheManager,thisTran).getSpecifyRow(key,thisTran);
        }

    }
}
