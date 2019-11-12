package npu.zunsql.treemng;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;

public class Node_index {
	
	//-1��ʾû������ҳ��û�ж��ӽڵ㣬�洢20����
	//-1��ʾû������ҳ
	//-1��ʾû������ҳ
	//-1��ʾû������ҳ
	//-1��ʾû������ҳ
	//-1��ʾû������ҳ
	//-1��ʾû������ҳ
	//-1��ʾû������ҳ
	
	public String tablename;

	public Integer pageOne;//���ǵ�ǰ�ڵ����ڵ�ҳ��
	
	public Integer sonpage;//��һ�����ӵ���ҳ��ҳ��
	
    public List<Integer> indexpages;//�������ĸ����ڵ�ҳ�Ź̶�һҳ�洢20��治�´浽sonpage��һҳ��sonpage�治��������

    public List<String> indexnames;//���������֣�����Ҫ�����潨�������е����֣����϶�Ӧ������������rowList1�е�Ԫ��һһ��Ӧ
    
    public List<List<String>> keynames;//Ҫ�����潨�������е�����
    
    public CacheMgr cacheManager;
    
    public Node_index(int thisPageID, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException 
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
        
        this.tablename=(String)objTable.readObject();
        objTable.readObject();//����ڵ�����ҳ��
        this.sonpage=(Integer)objTable.readObject();//���ӽڵ�����ҳ��
        this.indexpages=(List<Integer>)objTable.readObject();
        this.indexnames=(List<String>)objTable.readObject();
        this.keynames=(List<List<String>>)objTable.readObject();
    }
    
    public void intoBytes (Transaction thisTran) throws IOException 
    {
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);
        
        obj.writeObject(this.tablename);
        obj.writeObject(this.pageOne);
        obj.writeObject(this.sonpage);
        obj.writeObject(this.indexpages);
        obj.writeObject(this.indexnames);
        obj.writeObject(this.keynames);
        
        Page ppp=this.cacheManager.readPage(thisTran.tranNum, pageOne);
        ppp.getPageBuffer().rewind();
        ppp.getPageBuffer().put(byt.toByteArray());
        cacheManager.writePage(thisTran.tranNum,ppp);
    }
    
    public void add(int thisPageID, String indexname2,List<String> keynames2,CacheMgr cacheManager,Transaction thisTran) throws IOException, ClassNotFoundException
    {
    	if(this.indexnames.size()<20)
    	{
    		this.indexnames.add(indexname2);
    		this.indexpages.add(thisPageID);
    		this.keynames.add(keynames2);
    		this.intoBytes(thisTran);
    	}
    	
    	else if(this.sonpage==-1)
    	{
    		
    		ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
    		Page p1 = new Page(tempBuffer);
    		Integer pnumber;
    		pnumber=p1.getPageID();
    		ByteArrayOutputStream byt = new ByteArrayOutputStream();
    		ObjectOutputStream obj = new ObjectOutputStream(byt);
    		
    		obj.writeObject(this.tablename);
    		obj.writeObject(pnumber);
    		Integer random=-1;
    		obj.writeObject(random);
    		
    		List<Integer>l1=new ArrayList<Integer>();
    		List<String>l2=new ArrayList<String>();
    		List<List<String>>l3=new ArrayList<List<String>>();
    		l1.add(thisPageID);
    		l2.add(indexname2);
    		l3.add(keynames2);
    		
    		obj.writeObject(l1);
    		obj.writeObject(l2);
    		obj.writeObject(l3);
    		
    		tempBuffer.rewind();
    		tempBuffer.put(byt.toByteArray());
    		
    		cacheManager.writePage(thisTran.tranNum, p1);
    		
    		this.sonpage=pnumber;
    	}
    	else {
    		Node_index ni1=new Node_index(this.sonpage,cacheManager,thisTran);
    		if(ni1.indexnames.size()<20)
    		{
    			ni1.indexnames.add(indexname2);
        		ni1.indexpages.add(thisPageID);
        		ni1.keynames.add(keynames2);
        		ni1.intoBytes(thisTran);
    		}
    		
    		else {
    			ni1.add(thisPageID, indexname2, keynames2, cacheManager, thisTran);
    		}
    	}
    }
    
}
