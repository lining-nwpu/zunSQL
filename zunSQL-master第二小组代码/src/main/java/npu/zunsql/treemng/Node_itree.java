package npu.zunsql.treemng;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;

public class Node_itree {
	public String tablename;
	
	public String indexname;//索引的名字，不是要在上面建索引的列名

	public Integer pageOne;//就是当前节点所在的页号
    
	public Integer fatherNodeID;//值是-1时表示没有父节点
	
	public Integer M;//节点的阶
	
	public List<String> keyname;//树的关键节点的列名
	
	public List<String> columnname;//节点的所有元素的列名称
	
	public List<Row>rowList;//所有行的具体内容
	
	public List<Integer>sonNodeList;
	
	public Node_itree rhizine;//总是用在根节点，其他节点用不到
	
	List<Integer> keyps=new ArrayList<Integer>();
	
	public Integer adjust_position=0;
	
    public Integer left_or_right=0;
	
	public Integer search_position=0;
	
	public Integer counter=0;
	
	public Integer stack_auto_top=0;
	
	public Node_itree[] stack_auto;
	
    public CacheMgr cacheManager;
    
    public Node_itree(int thisPageID, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException 
    {
        this.cacheManager = cacheManager;
        Page ppp=this.cacheManager.readPage(thisTran.tranNum, thisPageID);
        this.pageOne=ppp.getPageID();
        
        /*if(this.pageOne==25)
        {
        	System.out.println("index lk");
        }*/
        
        ByteBuffer thisBufer = ppp.getPageBuffer();
        byte [] bytes=new byte[Page.PAGE_SIZE] ;
        thisBufer.rewind();
        thisBufer.get(bytes,0,thisBufer.remaining());

        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
        ObjectInputStream objTable=new ObjectInputStream(byteTable/*new BufferedInputStream(new ByteArrayInputStream(bytes))*/);
        
        this.tablename=(String)objTable.readObject();
        this.indexname=(String)objTable.readObject();
        this.pageOne=(Integer)objTable.readObject();//本身节点所在页号
        this.fatherNodeID=(Integer)objTable.readObject();//父亲节点所在页号
        this.M=(Integer)objTable.readObject();
        this.keyname=(List<String>)objTable.readObject();
        this.columnname=(List<String>)objTable.readObject();
        this.rowList=(List<Row>)objTable.readObject();
        this.sonNodeList=(List<Integer>)objTable.readObject();
        this.rhizine=this;
    }
    
    public Node_itree(int order2,String tablename2,String indexname2,List<String> keyname2,List<String>list1,CacheMgr cacheManager, Transaction thisTran) throws IOException 
    {
    	ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
    	this.tablename=tablename2;
    	this.indexname=indexname2;
    	

    	
    	Page ppp=new Page(buffer);
        this.pageOne = ppp.getPageID();
        this.fatherNodeID = -1;//用-1代表没有父亲
        this.M=order2;
        this.keyname=keyname2;
        this.columnname=list1;
        this.rowList=new ArrayList<Row>();
        this.sonNodeList = new ArrayList<Integer>();
        this.rhizine=this;
        this.cacheManager = cacheManager;
        
        /*if(this.pageOne==25)测试第25页
        {
        	System.out.println("index lk");
        }
        
        if(this.pageOne==44)
        {
        	if(this.rowList.size()==0)
        	{
        		System.out.println("空的");
        	}
        	if(this.sonNodeList.size()==0)
        	{
        		System.out.println("儿子也空的");
        	}
        	for(Integer i:this.sonNodeList)
        	{
        		System.out.println(i);
        	}
        	for(Row r1:this.rowList)
        	{
        		System.out.println(r1.getCell(0).getValue_s());
        		System.out.println(r1.getCell(1).getValue_s());
        		System.out.println(r1.getCell(2).getValue_s());
        		System.out.println(r1.getCell(3).getValue_s());
        	}
        }*/
        
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);
        obj.writeObject(this.tablename);
        obj.writeObject(this.indexname);
        obj.writeObject(this.pageOne);
        obj.writeObject(this.fatherNodeID);
        obj.writeObject(this.M);
        obj.writeObject(this.keyname);
        obj.writeObject(this.columnname);
        obj.writeObject(this.rowList);
        obj.writeObject(this.sonNodeList);
        buffer.rewind();
        buffer.put(byt.toByteArray());
        cacheManager.writePage(thisTran.tranNum,ppp);
    }
    
    public void intoBytes (Transaction thisTran) throws IOException 
    {
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);
        
        
        /*if(this.pageOne==44)
        {
        	if(this.rowList.size()==0)
        	{
        		System.out.println("intobytes空的");
        	}
        	if(this.sonNodeList.size()==0)
        	{
        		System.out.println("intobytes儿子也空的");
        	}
        	for(Integer i:this.sonNodeList)
        	{
        		System.out.println(i);
        	}
        	for(Row r1:this.rowList)
        	{
        		System.out.println(r1.getCell(0).getValue_s());
        		System.out.println(r1.getCell(1).getValue_s());
        		System.out.println(r1.getCell(2).getValue_s());
        		System.out.println(r1.getCell(3).getValue_s());
        	}
        	System.out.println("intobytes");
        }
        
        if(this.pageOne==25)
        {
        	System.out.println("index lk");
        }*/
        
        obj.writeObject(this.tablename);
        obj.writeObject(this.indexname);
        obj.writeObject(this.pageOne);
        obj.writeObject(this.fatherNodeID);
        obj.writeObject(this.M);
        obj.writeObject(this.keyname);
        obj.writeObject(this.columnname);
        obj.writeObject(this.rowList);
        obj.writeObject(this.sonNodeList);
        
        Page ppp=this.cacheManager.readPage(thisTran.tranNum, pageOne);
        ppp.getPageBuffer().rewind();
        ppp.getPageBuffer().put(byt.toByteArray());
        cacheManager.writePage(thisTran.tranNum,ppp);
    }
    
    public void update_root(Integer page,String name,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int search_position=-1000;
    	Node n1=new Node(-2,cacheManager,thisTran);//代表访问的是master根节点页号
    	Cell c1=new Cell(tablename);
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
    	
    	Node_index nin1=new Node_index(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s()),this.cacheManager,thisTran);
    	
    	while(true)
    	{
	    	if(nin1.indexnames.indexOf(name)==-1)
	    	{
	    		if(nin1.sonpage!=-1)
	    		{
	    			nin1=new Node_index(nin1.sonpage,this.cacheManager,thisTran);
	    			continue;
	    		}
	    		else {
	    			break;
	    		}
	    	}
	    	
	    	else{
	    		nin1.indexpages.set(nin1.indexnames.indexOf(name),page);	
	    		nin1.intoBytes(thisTran);
	    		break;
	    	}
    	}
    }
    
    public void split(Node_itree root,Transaction thisTran) throws IOException, ClassNotFoundException
    {
    	int i,j;
    	Row media;
    	Node_itree p1,p2;
    	p1=new Node_itree(M,tablename,this.indexname,keyname,this.columnname,cacheManager,thisTran);
    	
    	for(i=M/2+1,j=0;i<root.rowList.size();i++,j++)
    	{
    		p1.rowList.add(root.rowList.get(i));
    		p1.sonNodeList.add(root.sonNodeList.get(i));
    		p2=new Node_itree(root.sonNodeList.get(i),cacheManager,thisTran);
    		p2.fatherNodeID=p1.pageOne;
    		p2.intoBytes(thisTran);
    	}
    	p1.sonNodeList.add(root.sonNodeList.get(i));
/*这就要出事*/    	p2=new Node_itree(root.sonNodeList.get(i),cacheManager,thisTran);
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
			p2=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
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
			p2=new Node_itree(M,tablename,this.indexname,keyname,this.columnname,cacheManager,thisTran);
			p1.fatherNodeID=p2.pageOne;
			root.fatherNodeID=p2.pageOne;
			
			p2.rowList.add(media);
			p2.sonNodeList.add(root.pageOne);
			p2.sonNodeList.add(p1.pageOne);
			
			p1.intoBytes(thisTran);
			p2.intoBytes(thisTran);
			root.intoBytes(thisTran);
			
			this.rhizine=p2;
			update_root(p2.pageOne,this.indexname,thisTran);
		}
    }
    
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    //keyps不是自动赋值的，每次都要函数自己算一遍
    public boolean bigerThan(Row r1,Row r2)//r1是否大于r2
    {
    	int i;
    	for(i=0;i<keyps.size();i++)
    	{
    		if(r1.getCell(keyps.get(i)).bigerThan(r2.getCell(keyps.get(i))))
    		{
    			return true;
    		}
    		else if(r1.getCell(keyps.get(i)).letterThan(r2.getCell(keyps.get(i))))
    		{
    			return false;
    		}
    	}
    	return false;
    }
    
    public boolean letterThan(Row r1,Row r2)//r1是否小于r2
    {
    	int i;
    	for(i=0;i<keyps.size();i++)
    	{
    		if(r1.getCell(keyps.get(i)).letterThan((r2.getCell(keyps.get(i)))))
    		{
    			return true;
    		}
    		else if(r1.getCell(keyps.get(i)).bigerThan(r2.getCell(keyps.get(i))))
    		{
    			return false;
    		}
    	}
    	return false;
    }
    
    public boolean equalTo(Row r1,Row r2)//r1是否等于r2
    {
    	int i;
    	for(i=0;i<keyps.size();i++)
    	{
    		if(r1.getCell(keyps.get(i)).equalTo(r2.getCell(keyps.get(i))))
    		{
    			continue;
    		}
    		else{
    			return false;
    		}
    	}
    	return false;
    }
    
    //由于rhizine的影响，只允许用根所在的节点进行insert操作
    public boolean insertRow(Row row,Transaction thisTran) throws IOException, ClassNotFoundException {
        boolean insertOrNot = false;//row里的s1假设一共有2*n+1个元素，前n个元素是插入列具体值，后n个元素是插入列列名，最后一个元素是插入表的
                                    //主键的列名（只有一个名字，一个主键）
                                    //rowList的还是不会用这种方式储存，而是只是存key值  
        int insertNumber = 0;
        
        keyps.clear();
        
        for(int i=0;i<keyname.size();i++)
        {
        	keyps.add(null);
        }
        
        for(int i=0,j=0;i<this.columnname.size();i++)
        {
        	if(this.columnname.get(i).contentEquals(keyname.get(j)))
        	{
        		keyps.set(j, i);
        		j++;
        	}
        }
        
        List<Row> rowList2;
        Node_itree n1;
        rowList2=this.rhizine.rowList;
        n1=this.rhizine;
        while(n1.sonNodeList.size()!=0)
        {
        	int i;
        	for(i=0;i<rowList2.size();i++)
	        {
	        	if(bigerThan(rowList2.get(i),row))
	        	{
	        		break;
	        	}
	        }
        	n1=new Node_itree(n1.sonNodeList.get(i),cacheManager,thisTran);
        	rowList2=n1.rowList;
        }
        
        int i;
        
        int size=rowList2.size();
        
        rowList2.add(null);
        
        for(i=size-1;i>=0;i--)
        {
        	if(bigerThan(rowList2.get(i),row))
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
        	Node_itree n2=new Node_itree(M,this.tablename,this.indexname,this.keyname,this.columnname,cacheManager,thisTran);
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
        		Node_itree n3;
        		n3=new Node_itree(n1.fatherNodeID,cacheManager,thisTran);
        		while(!n3.sonNodeList.get(i).equals(n1.pageOne))//是这里427
        		{
        			i++;
        		}
        		
        		n3.rowList.add(media);//随便加了一个元素为了使得n3的size加1
        		n3.sonNodeList.add(-1);//随便加了一个元素为了使得n3的size加1
        		
        		j=n3.sonNodeList.size()-2;//在for循环的前面好像没有用
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
        		Node_itree n3=new Node_itree(M,this.tablename,this.indexname,this.keyname,this.columnname,cacheManager,thisTran);
        		n3.rowList.add(media);
        		n3.sonNodeList.add(n1.pageOne);
        		n3.sonNodeList.add(n2.pageOne);
        		n1.fatherNodeID=n3.pageOne;
        		n2.fatherNodeID=n3.pageOne;
        		n1.intoBytes(thisTran);
        		n2.intoBytes(thisTran);
        		n3.intoBytes(thisTran);
        		
        		this.rhizine=n3;
        		update_root(n3.pageOne,this.indexname,thisTran);
        	}
        }
        
        return true;
    }  
    
    /////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////
    
    
    ////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////
    public Node_itree get_brother_Node_itree(Node_itree root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node_itree p1;
    	if(root.fatherNodeID!=-1)
    	{
    		Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
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
    			p1=new Node_itree(root_father.sonNodeList.get(i-1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=0;
	    			return p1;
	    		}
    		}
    		
    		if(i<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node_itree(root_father.sonNodeList.get(i+1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=1;
	    			return p1;
	    		}	
    		}
    	}
    	return null;
    }
    
    public void adjust(Node_itree root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i,j;
    	Node_itree p1,p2;
    	p1=get_brother_Node_itree(root,thisTran);
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
    			
    			
    			Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.set(0, root_father.rowList.get(adjust_position-1));
    			
    			if(p1.sonNodeList.size()!=0)//叶子节点有儿子，不为空
    			{
    				root.sonNodeList.set(0, p1.sonNodeList.get(p1.sonNodeList.size()-1));
    				p2=new Node_itree(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
    				p2.fatherNodeID=root.pageOne;///////////////////////aaaaaazaaaaaaaaaaaa
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
    			Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.add(null);
    			root.rowList.set(root.rowList.size()-1, root_father.rowList.get(adjust_position));

    			if(p1.sonNodeList.size()!=0)
    			{
    				root.sonNodeList.add(null);
    				root.sonNodeList.set(root.sonNodeList.size()-1, p1.sonNodeList.get(0));
    				p2=new Node_itree(p1.sonNodeList.get(0),cacheManager,thisTran);
    				p2.fatherNodeID=root.pageOne;//////////////////////////////
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
    		Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
    		if(adjust_position>0)
    		{
    			p1=new Node_itree(root_father.sonNodeList.get(adjust_position-1),cacheManager,thisTran);
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
    					p2=new Node_itree(root.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=p1.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(root.sonNodeList.size()!=0)
				{
					p1.sonNodeList.add(root.sonNodeList.get(j));
					p2=new Node_itree(root.sonNodeList.get(j),cacheManager,thisTran);
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
					update_root(p1.pageOne, this.indexname, thisTran);
				}
				else if(root_father.rowList.size()<M/2)
				{
					adjust(root_father,thisTran);
				}
    		}
    		else if(adjust_position<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node_itree(root_father.sonNodeList.get(adjust_position+1),cacheManager,thisTran);
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
    					p2=new Node_itree(p1.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=root.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(p1.sonNodeList.size()!=0)
				{
					root.sonNodeList.add(p1.sonNodeList.get(j));
					p2=new Node_itree(p1.sonNodeList.get(j),cacheManager,thisTran);
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
					update_root(root.pageOne,this.indexname,thisTran);
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
    
    //扔进去一个键值就让删除，这个主键值对应的行，没有其他辅助
    public boolean deleteRow(Row key,Transaction thisTran) throws IOException, ClassNotFoundException 
    {
        
    	for(int i=0;i<keyname.size();i++)
        {
        	keyps.add(null);
        }
        
        for(int i=0,j=0;i<this.columnname.size();i++)
        {
        	if(this.columnname.get(i).contentEquals(keyname.get(j)))
        	{
        		keyps.set(j, i);
        		j++;
        	}
        }
    	
    	
    	int i;
        int p1_position=0;
        Node_itree p1,p2;
        p1=this.rhizine;
        while(true)
        {
        	for(i=0;i<p1.rowList.size();i++)
        	{
        		if(bigerThan(p1.rowList.get(i),key))
        		{
        			break;
        		}
        	}
        	if(i>0&&equalTo(p1.rowList.get(i-1),key))
        	{
        		p1_position=i-1;
        		break;
        	}
        	else {
        		if(i<p1.sonNodeList.size())
        		{
        			p1=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
        		}
        		else {
        			p1=null;
        			break;
        		}
        	}
        }
        
        if(p1!=null&&p1.sonNodeList.size()!=0)
        {
        	p2=new Node_itree(p1.sonNodeList.get(p1_position),cacheManager,thisTran);
        	
        	while(p2.sonNodeList.size()!=0)
        	{
        		p2=new Node_itree(p2.sonNodeList.get(p2.sonNodeList.size()-1),cacheManager,thisTran);
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
        	for(i=p1_position;i<p1.rowList.size()-1;i++)//set改了
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
    public Node_itree search_auto(Row key,Node_itree root,Transaction thisTran) throws ClassNotFoundException, IOException//返回NOde在Node中的位置在search_position中
    {
    	int i;
    	Node_itree p1;
    	p1=root;
    	while(true)
    	{
    		for(i=0;i<p1.rowList.size();i++)
    		{
    			if(bigerThan(p1.rowList.get(i),key))
    			{
    				break;
    			}
    		}
    		if(i>0&&equalTo(p1.rowList.get(i-1),key))
    		{
    			search_position=i-1;
    			break;
    		}
    		else {
    			if(p1.sonNodeList.size()!=0)
    			{
    				p1=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
    			}
    			else {
    				p1=null;
    				break;
    			}
    		}
    	}
    	return p1;
    }
    
    public void search_all(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node_itree p1;
    	this.stack_auto_top=0;
    	this.stack_auto=new Node_itree[10000];
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
    					this.stack_auto[this.stack_auto_top]=new Node_itree(p1.sonNodeList.get(i+1),cacheManager,thisTran);
    					this.stack_auto_top++;
    					if(flag==0)
    					{
    						flag=1;
    						this.stack_auto[this.stack_auto_top]=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
    						this.stack_auto_top++;
    					}
    					/////对相等的key的节点的处理todo
    				}
    				else if(p1.rowList.get(i).getCell(keyp).bigerThan(key))
    				{
    					if(flag==0)
    					{
    						flag=2;
    						this.stack_auto[this.stack_auto_top]=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
    						this.stack_auto_top++;
    					}
    					break;
    				}
    			}
    			if(flag==0)
    			{
    				this.stack_auto[this.stack_auto_top]=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
    				this.stack_auto_top++;
    			}
    		}
    	}
    	this.stack_auto=null;
    }
    
    public void delete_all(Node_itree root, Row key, Integer keyp,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	
    	
    	this.counter=0;
    	count_key_number(root,key,keyp,thisTran);
    	for(i=0;i<this.counter;i++)
    	{
    		root.deleteRow(key,thisTran);
    	}
    }
    
/////////////////////////////////
    //count之前一定要置0
    ////////////////////////////////////////////
    //count之前一定要置0
    	////////////////////////////////////
    public void count_key_number(Node_itree root, Row key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {//count之前一定要置0
    	//count之前一定要置0
    	int i;
    	Node_itree p1;
    	for(i=0;i<root.rowList.size();i++)
    	{
    		if(equalTo(root.rowList.get(i),key))
            {
            	counter++;
            }
    		else if(bigerThan(root.rowList.get(i),key))
    		{
    			break;
    		}
    	}
    	for(i=0;i<root.sonNodeList.size();i++)
    	{
    		p1=new Node_itree(root.sonNodeList.get(i),cacheManager,thisTran);
    		count_key_number(p1,key,keyp,thisTran);
    	}
    }
    
    public void get_node_all(Node_itree root, Transaction thisTran) throws ClassNotFoundException, IOException//得到节点的所有行和节点所有儿子的所有行
    {
    	int i;
    	Node_itree p1,p2;
    	p1=root;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
    	s1.push(p1);
    	while(!s1.empty())
    	{
    		p1=s1.pop();
    		if(p1.sonNodeList.size()!=0)
    		{
    			p2=new Node_itree(p1.sonNodeList.get(0),cacheManager,thisTran);
    			s1.push(p2);
	    		for(i=0;i<p1.rowList.size();i++)
	    		{
	    			cacheManager.search_result(p1.rowList.get(i));
	    			p2=new Node_itree(p1.sonNodeList.get(i+1),cacheManager,thisTran);
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
    
    public void search_greater_all(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException//不一定是主键了
    {
    	int i;
    	int flag=0;
    	Node_itree p1,p2;
    	p1=root;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
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
						p2=new Node_itree(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
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
						p2=new Node_itree(p1.sonNodeList.get(i+1), cacheManager, thisTran);
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
					p2=new Node_itree(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
					s1.push(p2);
				}
			}
		}
    	s1=null;
    }
    
    public void search_letter_all(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
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
    					p2=new Node_itree(p1.sonNodeList.get(i+1),cacheManager,thisTran);
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
	    				p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
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
    				p2=new Node_itree(p1.sonNodeList.get(0),cacheManager,thisTran);
					s1.push(p2);
    			}
    		}
    	}
    	s1=null;
    }
    
    public void search_equal_all(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
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
    					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
    					s1.push(p2);
    				}
    			}
    			else if(p1.rowList.get(i).getCell(keyp).bigerThan(key))
				{
    				flag=2;
    				if(p1.sonNodeList.size()!=0)
    				{
    					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
    					s1.push(p2);
    				}
    				break;
				}
    		}
    		if(flag!=2)
    		{
				if(p1.sonNodeList.size()!=0)
				{
					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
				}
    		}
    	}
    	s1=null;
    }
    
    public void search_greater_or_equal_all(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
    	
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
    					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
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
	    				p2=new Node_itree(p1.sonNodeList.get(i+1),cacheManager,thisTran);
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
    				p2=new Node_itree(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
    				s1.push(p2);
    			}
    		}
    	}
    	s1=null;
    }
    
    public void search_letter_or_equal_all(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	int flag;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
    	
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
    					p2=new Node_itree(p1.sonNodeList.get(i+1),cacheManager,thisTran);
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
	    				p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
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
    				p2=new Node_itree(p1.sonNodeList.get(0),cacheManager,thisTran);
    				s1.push(p2);
    			}
    		}
    	}
    	s1=null;
    }

    
    ///////////////////////////////////////////////////////////////////
    //上面是按主键可以搜索的，下面是不可以按主键搜索的
    ///////////////////////////////////////////////////////////////////
    
    public void search_greater_all_nokey(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException//不一定是主键了
    {
    	int i;
    	Node_itree p1,p2;
    	p1=root;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
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
					p2=new Node_itree(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
					s1.push(p2);
				}
	    		p2=new Node_itree(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
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
    
    public void search_letter_all_nokey(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
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
	    			p2=new Node_itree(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
					s1.push(p2);
	    		}
	    		p2=new Node_itree(p1.sonNodeList.get(i), cacheManager, thisTran);//节点左侧要被搜索
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
    
    public void search_equal_all_nokey(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
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
					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
	    		}
				p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
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
    
    public void search_greater_or_equal_all_nokey(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
    	
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
					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
	    		}
				p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
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
    
    public void search_letter_or_equal_all_nokey(Node_itree root, Cell key, Integer keyp, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node_itree p1,p2;
    	Stack<Node_itree>s1=new Stack<Node_itree>();
    	
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
					p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
					s1.push(p2);
	    		}
				p2=new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran);
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
    
    public Node_itree get_brother_Node_nokey(Node_itree root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i;
    	Node_itree p1;
    	if(root.fatherNodeID!=-1)
    	{
    		Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
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
    			p1=new Node_itree(root_father.sonNodeList.get(i-1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=0;
	    			return p1;
	    		}
    		}
    		
    		if(i<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node_itree(root_father.sonNodeList.get(i+1),cacheManager,thisTran);
	    		if(p1.rowList.size()>M/2)
	    		{
	    			left_or_right=1;
	    			return p1;
	    		}
    		}
    	}
    	return null;
    }
    
    public void adjust_nokey(Node_itree root,Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i,j;
    	Node_itree p1,p2;
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
    			
    			Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.set(0, root_father.rowList.get(adjust_position-1));
    			
    			if(p1.sonNodeList.size()!=0)//叶子节点有儿子，不为空
    			{
    				root.sonNodeList.set(0, p1.sonNodeList.get(p1.sonNodeList.size()-1));
    				p2=new Node_itree(p1.sonNodeList.get(p1.sonNodeList.size()-1),cacheManager,thisTran);
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
    			Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
    			root.rowList.add(null);
    			root.rowList.set(root.rowList.size()-1, root_father.rowList.get(adjust_position));
    			if(p1.sonNodeList.size()!=0)
    			{
    				root.sonNodeList.add(null);
    				root.sonNodeList.set(root.sonNodeList.size()-1, p1.sonNodeList.get(0));
    				p2=new Node_itree(p1.sonNodeList.get(0),cacheManager,thisTran);
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
    		Node_itree root_father=new Node_itree(root.fatherNodeID,cacheManager,thisTran);
    		if(adjust_position>0)
    		{
    			p1=new Node_itree(root_father.sonNodeList.get(adjust_position-1),cacheManager,thisTran);
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
    					p2=new Node_itree(root.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=p1.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(root.sonNodeList.size()!=0)
				{
					p1.sonNodeList.add(root.sonNodeList.get(j));
					p2=new Node_itree(root.sonNodeList.get(j),cacheManager,thisTran);
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
					update_root(p1.pageOne, this.indexname, thisTran);
				}
				else if(root_father.rowList.size()<M/2)
				{
					adjust_nokey(root_father,thisTran);
				}
    		}
    		else if(adjust_position<root_father.sonNodeList.size()-1)
    		{
    			p1=new Node_itree(root_father.sonNodeList.get(adjust_position+1),cacheManager,thisTran);
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
    					p2=new Node_itree(p1.sonNodeList.get(j),cacheManager,thisTran);
    					p2.fatherNodeID=root.pageOne;
    					p2.intoBytes(thisTran);
    				}
    			}
				if(p1.sonNodeList.size()!=0)
				{
					root.sonNodeList.add(p1.sonNodeList.get(j));
					p2=new Node_itree(p1.sonNodeList.get(j),cacheManager,thisTran);
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
					update_root(root.pageOne,this.indexname,thisTran);
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
    
    public boolean deleteRow_nokey(Node_itree root,Integer p1_position_arg,Transaction thisTran) throws IOException, ClassNotFoundException {
        int i;
        int p1_position=0;
        Node_itree p1,p2;
        
        p1=root;
        
        p1_position=p1_position_arg;
        
        if(p1!=null&&p1.sonNodeList.size()!=0)
        {
        	p2=new Node_itree(p1.sonNodeList.get(p1_position),cacheManager,thisTran);
        	
        	while(p2.sonNodeList.size()!=0)
        	{
        		p2=new Node_itree(p2.sonNodeList.get(p2.sonNodeList.size()-1),cacheManager,thisTran);
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
    
    public void deleteRow_nokey_pre(Node_itree root, List<Cell> keys, List<Integer> keyps, List<String> condition, Transaction thisTran) throws ClassNotFoundException, IOException
    {
    	int i,j;
    	int flag;
    	Node_itree p1;
    	Stack<Node_itree> st1=new Stack<Node_itree>();
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
	    			st1.push(new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran));
	        	}
    			st1.push(new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran));
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
    
    public void count_key_number_nokey(Node_itree root, List<Cell> keys, List<Integer> keyps, List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
    {//count之前一定要置0
    	//count之前一定要置0
    	int i,j;
    	int flag;
    	Node_itree p1;
    	Stack<Node_itree> st1=new Stack<Node_itree>();
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
	    			st1.push(new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran));
	        	}
    			st1.push(new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran));
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
    public void delete_all_nokey(Node_itree root, List<Cell> keys, List<Integer> keyps,List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
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
    
    
    public void count_key_number_nokey2(Node_itree root, List<Cell> keys, List<Integer> keyps, List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
    {//count之前一定要置0
    	//count之前一定要置0
    	int i,j;
    	int flag;
    	Node_itree p1;
    	Stack<Node_itree> st1=new Stack<Node_itree>();
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
	    			st1.push(new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran));
	        	}
    			st1.push(new Node_itree(p1.sonNodeList.get(i),cacheManager,thisTran));
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
    public void delete_all_nokey2(Node_itree root, List<Cell> keys, List<Integer> keyps, List<String> condition,Transaction thisTran) throws ClassNotFoundException, IOException
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
    
    public void delete_zheng_ge_shu_bao_cun_geng_ye(Node_itree root,Transaction thisTran) throws ClassNotFoundException, IOException//也保存到内存的更新表中
    {
    	int i;
    	Node_itree n1;
    	Stack<Node_itree>st1=new Stack<Node_itree>();
    	n1=root;
    	
    	for(i=0;i<n1.sonNodeList.size();i++)
    	{
    		st1.push(new Node_itree(n1.sonNodeList.get(i),this.cacheManager,thisTran));
    	}
    	
    	n1.sonNodeList.clear();
    	n1.rowList.clear();
    	n1.fatherNodeID=-1;
    	n1.rhizine=n1;
    	n1.intoBytes(thisTran);
    	
    	while(!st1.empty())
    	{
    		n1=st1.pop();
			for(i=0;i<n1.sonNodeList.size();i++)
        	{
        		st1.push(new Node_itree(n1.sonNodeList.get(i),this.cacheManager,thisTran));
        	}
			this.cacheManager.unusedList_PageID.add(n1.pageOne);
    	}
    }
    
}


