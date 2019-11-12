package npu.zunsql.virenv;

import npu.zunsql.cache.Page;
import npu.zunsql.treemng.*;
//import sun.security.provider.JavaKeyStore.CaseExactJKS;
//import sun.security.provider.JavaKeyStore.CaseExactJKS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;


//import javax.sound.sampled.Port.Info;

public class VirtualMachine {
	public int flag;//在函数中有注释用在select语句中
	
	public int is_update;
	
	public int is_insert;
	
	public long start;
	
	public long end;
	
	public int []index_key_nokey=new int[200];//0用索引搜索，1用主键搜索，2没有用搜索
	
	public List<String>key_names=new ArrayList<String>();
	
	public List<List<String>> index_names=new ArrayList<List<String>>();
	
	public int top1;
	
	public boolean is_whole_not_null=false;
	
	public List<String> index_name;//用在建立的索引的名字
	
	public List<String> indexname_list;//用在index括号后面建立索引的列中
	
	public List<String> tablename_list;//用在select中存储from后涉及了多少表
	
	public List<String> where_condition;//用在select中存储where后每个列选择条件形式是
										//op1的字符串，op2的字符串，两个操作数的比较方式，和后面条件的连接方式，例如scno 201708 EQ and/or null
										//用null区分一组列选择    第一组null第二组null第三组null
	
										//修改后使用select中存储where后每个列选择条件形式是
										//op1的字符串，op2的字符串，两个操作数的比较方式，如scno 201708 EQ null
										//用null区分一组列选择    第一组null第二组null第三组null
	
	public List<String> update_condition;//用在update中存储set后面每个列置位条件形式是
										 //op1的字符串，op2的字符串，例如 score 666 null
										 //用null区分一组列选择 第一组null第二组null第三组null
	
	private List<String> selectedColumns;//存储select中select后的各个列名
	
	public List<String> insert_column_names;//存储表名(后要插入的列的名字不是值
	
	public List<String> insert_column_values;//存储values(后的具体列的值 
	
	private List<EvalDiscription> filters;

	private List<AttrInstance> record;
	
	private List<Column> columns;
	
	private QueryResult result;
	
	private String targetTable;
	
	private String pkName;
	
	private List<String> updateAttrs;
	
	private List<List<EvalDiscription>> updateValues;
	
	private List<EvalDiscription> singleUpdateValue;
	
	private Activity activity;
	
	private QueryResult joinResult;
	
	public Transaction tran;
	private Transaction usertran;

	private boolean isJoin = false;
	private int joinIndex = 0;

	private boolean suvReadOnly;
	private boolean recordReadOnly;
	private boolean columnsReadOnly;
	private boolean selectedColumnsReadOnly;
	private Database db;

	private boolean isUserTransaction = false;
	
	public List<Node_itree>nit2=new ArrayList<Node_itree>();

	public VirtualMachine(Database pdb) {
		recordReadOnly = true;
		columnsReadOnly = true;
		selectedColumnsReadOnly = true;
		suvReadOnly = true;

		tran = null;
		result = null;
		activity = null;
		targetTable = null;
		joinResult = null;

		filters = new ArrayList<>();
		selectedColumns = new ArrayList<String>();
		record = new ArrayList<>();
		columns = new ArrayList<>();
		updateAttrs = new ArrayList<>();
		updateValues = new ArrayList<>();
		singleUpdateValue = new ArrayList<>();
		tablename_list=new ArrayList<String>();
		where_condition=new ArrayList<String>();
		update_condition=new ArrayList<String>();
		this.indexname_list=new ArrayList<String>();
		this.index_name=new ArrayList<String>();
		this.insert_column_names=new ArrayList<String>();
		this.insert_column_values=new ArrayList<String>();

		pkName = null;
		db = pdb;

		usertran = null;
		isUserTransaction = false;
	}

	public QueryResult run(List<Instruction> instructions) throws Exception 
	{

		flag=0;//用在判断Beginfilter是否是第一次执行到以及，Beginfilter对where两个operand
		       //用到的到底是p1还是p2用来判断select时应该在where_condition中存储p1还是p2 
			   //用0代表才开始，1代表应该存p1，2代表应该存p2
		
		this.is_update=0;
		this.is_insert=0;
		for (Instruction cmd : instructions) 
		{
			//System.out.println(cmd.opCode + " " + cmd.p1 + " " + cmd.p2 + " " + cmd.p3);乱打
			run(cmd);
		}
		//System.out.println("\n");乱打
		return result;
	}

	private void run(Instruction instruction) throws IOException, ClassNotFoundException {
		OpCode opCode = instruction.opCode;
		String p1 = instruction.p1;
		String p2 = instruction.p2;
		String p3 = instruction.p3;

		switch (opCode) {
		
		case Transaction:
			ConditonClear();
			break;

		case Begin:
			tran = db.beginWriteTrans();
			isUserTransaction = true;
			break;

		case UserCommit:
			try {
				tran.Commit();
			} 
			catch (IOException e) 
			{
				Util.log("閹绘劒姘︽径杈Е");
				throw e;
			}
			tran = null;
			isUserTransaction = false;
			break;

		case Commit:
			try {
				tran.Commit();
				ConditonClear();
			} 
			catch (IOException e) 
			{
				Util.log("閹绘劒姘︽径杈Е");
				throw e;
			}
			break;

		/*case Rollback:
			tran.RollBack();
			isUserTransaction = false;
			try {
				db.close();
				db = new Database(db.getDatabaseName());
			} 
			catch (IOException ie) 
			{
				ie.printStackTrace();
				System.exit(-1);
			} 
			catch (ClassNotFoundException ce) 
			{
				ce.printStackTrace();
				System.exit(-1);
			}
			break;*/

		case CreateTable:
			columns.clear();
			activity = Activity.CreateTable;
			columnsReadOnly = false;
			targetTable = p3;
			break;
			
		case Index:
			activity = Activity.Create_Index;
			this.index_name.add(p1);
			this.tablename_list.add(p2);
			break;
			
		case Index_Column:
			this.indexname_list.add(p1);
			break;

		case AddCol:
			columns.add(new Column(p1, p2));
			break;

		case BeginPK:
			columnsReadOnly = true;
			break;

		case AddPK:
			pkName = p1;
			break;

		case EndPK:
			if(pkName==null)
			{
				pkName=columns.get(0).ColumnName;
			}
			break;
			
		case DropTable:
			activity = Activity.DropTable;
			targetTable = p3;
			break;
			
		case Insert:
			activity = Activity.Insert;
			targetTable = p3;
			this.tablename_list.add(p3);
			record.clear();
			updateValues.clear();
			this.is_insert=1;
			break;

		case Delete:
			activity = Activity.Delete;
			targetTable = p3;
			this.tablename_list.add(p3);
			break;

		case Select:
			activity = Activity.Select;
			// targetTable = p3;
			break;
			
		case Update:
			activity = Activity.Update;
			targetTable = p3;
			this.tablename_list.add(p3);
			break;

		case BeginItem:
			recordReadOnly = false;
			break;

		case AddItemCol:
			record.add(new AttrInstance(p1, p2, p3));
			this.insert_column_names.add(p1);
			break;

		case EndItem:
			recordReadOnly = true;
			break;

		case BeginFilter:
			suvReadOnly = false;
			singleUpdateValue = new ArrayList<>();
			break;

		case EndFilter:
			filters = singleUpdateValue;
			// System.out.println("filters name"+filters.get(0).col_name);
			suvReadOnly = true;
			break;

		case BeginColSelect:
			selectedColumnsReadOnly = false;
			break;

		case AddColSelect:
			selectedColumns.add(p1);
			break;

		case EndColSelect:
			selectedColumnsReadOnly = true;
			break;

		case BeginJoin:
			joinResult = null;
			isJoin = true;
			joinIndex = 0;
			if (!isUserTransaction) 
			{
				tran = db.beginReadTrans();
			}
			break;

		case AddTable:
			targetTable = p1;
			this.tablename_list.add(p1);
			//修改的部分
			//join(targetTable);
			break;

		case EndJoin:
			break;

		case Set:
			updateAttrs.add(p1);
			this.is_update=1;
			this.update_condition.add(p1);
			break;

		case BeginExpression:
			// updateValues.clear();
			suvReadOnly = false;
			singleUpdateValue = new ArrayList<>();
			break;

		case EndExpression:
			updateValues.add(singleUpdateValue);
			suvReadOnly = true;
			break;

		case Operand:
			singleUpdateValue.add(new EvalDiscription(opCode, p1, p2));
			if(this.is_insert==0)
			{
				if(this.is_update==0)
				{
					if(flag==0)
					{
						flag=2;
						where_condition.add(p1);
					}
					else if(flag==1)
					{
						flag=2;
						where_condition.add(p1);
					}
					else if(flag==2)
					{
						flag=1;
						where_condition.add(p2);
					}
				}
				else {
					this.is_update=0;
					this.update_condition.add(p2);
					this.update_condition.add(null);
				}
			}
			else {
				this.insert_column_values.add(p2);
			}
			break;

		case Operator:
			singleUpdateValue.add(new EvalDiscription(OpCode.valueOf(p1), null, null));
			where_condition.add(p1);			
			if(p1.equalsIgnoreCase("or")||p1.equalsIgnoreCase("and"))
			{
				where_condition.add(null);
			}
			break;

		case Execute:
			execute();	
			break;

		default:
			Util.log("濞屸剝婀佹潻娆愮壉閻ㄥ嫬鐡ч懞鍌滅垳: " + opCode + " " + p1 + " " + p2 + " " + p3);
			break;

		}
	}

	private void ConditonClear() throws IOException, ClassNotFoundException {
		recordReadOnly = true;
		columnsReadOnly = true;
		selectedColumnsReadOnly = true;
		suvReadOnly = true;
		filters.clear();

		// tran = null;
		// result = null;
		selectedColumns.clear();
		record.clear();
		columns.clear();
		updateAttrs.clear();
		updateValues.clear();
		singleUpdateValue.clear();
		this.where_condition.clear();
		this.tablename_list.clear();
		this.selectedColumns.clear();
		this.update_condition.clear();
		this.index_name.clear();
		this.indexname_list.clear();
		this.insert_column_names.clear();
		this.insert_column_values.clear();
		activity = null;
		targetTable = null;
		joinResult = null;
	}

	private void execute() throws IOException, ClassNotFoundException {
		result = new QueryResult();
		switch (activity) {
		case Select:
			this.top1=0;
			this.index_names.clear();
			this.key_names.clear();
			select();
			// ConditonClear();
			isJoin = false;
			break;
			
		case Delete:
			delete();
			break;
			
		case Update:
			update();
			break;
			
		case Insert:
			insert();
			updateValues.clear();
			break;
			
		case CreateTable:
			createTable();
			break;
			
		case Create_Index:
			create_Index();
			break;
			
		case DropTable:
			dropTable();
			break;
			
		default:
			break;
		}
	}

	public void create_Index() throws ClassNotFoundException, IOException
	{
		int i;
		int search_position=-1000;
		Node n1=new Node(-2,db.cacheManager,tran);
    	Cell c1=new Cell(this.tablename_list.get(0));
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
    				n1=new Node(n1.sonNodeList.get(i),db.cacheManager,tran);
    			}
    			else {
    				break;
    			}
    		}
    	}
    	Integer pnumber;
    	pnumber=Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s());
    	
    	if(pnumber==-1)
    	{
    		ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
    		Page p1 = new Page(tempBuffer);
    		pnumber=p1.getPageID();
    		byte[] bytes;
    		ByteArrayOutputStream byt = new ByteArrayOutputStream();
    		ObjectOutputStream obj = new ObjectOutputStream(byt);
    		
    		obj.writeObject(this.tablename_list.get(0));
    		obj.writeObject(pnumber);
    		Integer random=-1;
    		obj.writeObject(random);
    		
    		///////////////////////////////////////////
    		////////////////////////////////////////////
    		
    		Node n2=new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()),db.cacheManager,tran);
    		
    		//新建不是读入
    		Node_itree nit1=new Node_itree(5,this.tablename_list.get(0),this.index_name.get(0),this.indexname_list,n2.columnname,db.cacheManager,tran);
    		
    		
    		//////////////////////////////////////////////
    		////////////////////////////////////////////
    		
    		List<Integer>l1=new ArrayList<Integer>();
    		List<String>l2=new ArrayList<String>();
    		List<List<String>>l3=new ArrayList<List<String>>();
    		l1.add(nit1.pageOne);
    		l2.add(this.index_name.get(0));
    		l3.add(indexname_list);
    		
    		obj.writeObject(l1);
    		obj.writeObject(l2);
    		obj.writeObject(l3);
    		
    		bytes=byt.toByteArray();
    		tempBuffer.put(bytes);
    		
    		db.cacheManager.writePage(tran.tranNum, p1);
    		
    		Cell ccc=n1.rowList.get(search_position).getCell(2);
    		
    		ccc.sValue=pnumber.toString();
    		
    		//不用set
    		
    		n1.intoBytes(tran);
    
    		Node n3;
    		
    		Stack<Node>st1=new Stack<Node>();
    		
    		st1.push(n2);
    		
    		while(!st1.empty())
    		{
    			n3=st1.pop();
    			if(n3.sonNodeList.size()!=0)
    			{
    				for(i=0;i<n3.rowList.size();i++)
	    			{
	    				nit1.insertRow(n3.rowList.get(i), tran);
	    				st1.push(new Node(n3.sonNodeList.get(i),db.cacheManager,tran));
	    			}
    				st1.push(new Node(n3.sonNodeList.get(i),db.cacheManager,tran));
    			}
    			
    			else if(n3.sonNodeList.size()==0) 
    			{
    				for(i=0;i<n3.rowList.size();i++)
	    			{
	    				nit1.insertRow(n3.rowList.get(i), tran);
	    			}
    			}
    		}
    		
    	}
    	
    	else {
    		Node n2=new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()),db.cacheManager,tran);
    		
    		//新建不是读入
    	
    		Node_index ni1=new Node_index(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s()),db.cacheManager,tran);
    		
    		
    		
    		byte [] bytes=new byte[Page.PAGE_SIZE];
    		
    		Integer ii1;
    		
    		
    		List<String>ll1;
    		List<Integer>ll2;
    		List<List<String>>ll3;
    		
    		Node_index ni2=ni1;
    		
    		while(true)
    		{
    			Page ppp=db.cacheManager.readPage(tran.tranNum, ni2.pageOne);
    			
    			ByteBuffer thisBufer = ppp.getPageBuffer();
		       
		        thisBufer.rewind();
		        thisBufer.get(bytes,0,thisBufer.remaining());
	
		        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
		        ObjectInputStream objTable=new ObjectInputStream(byteTable);
		        
		        objTable.readObject();
		        objTable.readObject();//本身节点所在页号
		        ii1=(Integer)objTable.readObject();//儿子节点所在页号
		        ll2=(List<Integer>)objTable.readObject();
		        ll1=(List<String>)objTable.readObject();
		        ll3=(List<List<String>>)objTable.readObject();
		        
		        
		        for(i=0;i<20&&i<ll1.size();i++)
		        {
		        	if(ll1.get(i).contentEquals(this.index_name.get(0)))
		        	{
		        		System.out.println(this.index_name.get(0)+"索引已经被创建过，具体如下：");
		        		System.out.println("索引所在页："+ll2.get(i));
		        		
		        		int y2=1;
		        		
		        		for(String s1:ll3.get(i))
		        		{
		        			System.out.println("第"+y2+"个索引键："+s1);
		        			y2++;
		        		}
		        		
		        		System.out.println();
		        		
		        		return ;
		        	}
		        }
		        
		        if(ii1==-1)
		        {
		        	break;
		        }
		        
		        else {
		        	ni2=new Node_index(ii1,db.cacheManager,tran);
		        }
		        
    		}
    		
    		
    		Node_itree nit1=new Node_itree(5,this.tablename_list.get(0),this.index_name.get(0),this.indexname_list,n2.columnname,db.cacheManager,tran);
    		
    		ni1.add(nit1.pageOne, this.index_name.get(0), this.indexname_list, db.cacheManager, tran);
    		
    		Node n3;
    		Stack<Node>st1=new Stack<Node>();
    		st1.push(n2);
    		
    		while(!st1.empty())
    		{
    			n3=st1.pop();
    			if(n3.sonNodeList.size()!=0)
    			{
    				for(i=0;i<n3.rowList.size();i++)
    				{
    					nit1.insertRow(n3.rowList.get(i), tran);
    					st1.push(new Node(n3.sonNodeList.get(i),db.cacheManager,tran));
    				}
    				st1.push(new Node(n3.sonNodeList.get(i),db.cacheManager,tran));
    			}
    			
    			else if(n3.sonNodeList.size()==0) 
    			{
    				for(i=0;i<n3.rowList.size();i++)
	    			{
	    				nit1.insertRow(n3.rowList.get(i), tran);
	    			}
    			}
    		}
    		
    	}
	}
	
	private void dropTable() throws IOException, ClassNotFoundException 
	{
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}
		/*if (db.dropTable(targetTable, tran) == false) {
			Util.log("閸掔娀娅庣悰銊ャ亼鐠愶拷");
		}*/
	}

	private void createTable() throws IOException, ClassNotFoundException 
	{
		//
		if (!isUserTransaction) 
		{
			tran = db.beginWriteTrans();
		}

		List<String> headerName = new ArrayList<>();
		List<BasicType> headerType = new ArrayList<>();
		for (Column n : columns) 
		{
			// System.out.println("#######name:"+n.ColumnName+"##########");
			headerName.add(n.ColumnName);
			switch (n.getColumnType()) 
			{
				case "String":
					headerType.add(BasicType.String);
					break;
					
				case "Float":
					headerType.add(BasicType.Float);
					break;
					
				case "Integer":
					headerType.add(BasicType.Integer);
			}
		}
		
		db.createTable(targetTable, pkName, headerName, headerType, tran);

	}

	private boolean check(Cursor p) throws IOException, ClassNotFoundException {
		// 婵″倹鐏夊▽鈩冩箒where鐎涙劕褰為敍宀勫亝娑斿牐绻戦崶鐎焤ue閿涘苯宓嗙�佃澧嶉張澶庮唶瑜版洟鍏橀幍褑顢戦幙宥勭稊
		if (filters.size() == 0) {
			return true;
		}

		UnionOperand ans;
		if (isJoin)
			ans = eval(filters, joinIndex);
		else {
			ans = eval(filters, p);
			// System.out.println("this should show twice");
		}
		if (ans.getType() == BasicType.String) {
			Util.log("where鐎涙劕褰為惃鍕�冩潏鎯х础鏉╂柨娲栭崐闂寸瑝閼虫垝璐烻tring");
			return false;
		} else if (Math.abs(Double.valueOf(ans.getValue())) < 1e-10) {
			return false;
		} else {
			return true;
		}
	}

	public boolean bigerThan(String s1,String s2)//如果s1比s2大返回真，其余返回假
    {
    	if(s1.matches("-?[0-9]+.?[0-9]*")&&s2.matches("-?[0-9]+.?[0-9]*"))
    	{
    		double d1,d2;
    		d1=Double.parseDouble(s1);
    		d2=Double.parseDouble(s2);
    		if(d1>d2)
    		{
    			return true;
    		}
    		else {
    			return false;
    		}
    	}
        return s1.compareTo(s2) > 0;
    }

    public boolean letterThan(String s1,String s2)//如果s1比s2小返回真，否则返回假
    {
    	if(s1.matches("-?[0-9]+.?[0-9]*")&&s2.matches("-?[0-9]+.?[0-9]*"))
    	{
    		double d1,d2;
    		d1=Double.parseDouble(s1);
    		d2=Double.parseDouble(s2);
    		if(d1<d2)
    		{
    			return true;
    		}
    		else {
    			return false;
    		}
    	}
        return s1.compareTo(s2) < 0;
    }
    
    public boolean equalTo(String s1,String s2)
    {
         return s1.contentEquals(s2);
    }
    
    public void select() throws ClassNotFoundException, IOException
    {
    	int i,j,k,l;
    	int search_position=-1000;
    	
    	long time=0;
    	
    	Node n0=new Node(-2,db.cacheManager,tran);
    	
    	Node n1=n0;
    	
    	List<String> result1;
    	
    	List<String> result2=new ArrayList<String>();
		
		List<Node> root_node_table=new ArrayList<Node>();

		List<List<Node_itree>> nit3=new ArrayList<List<Node_itree>>();
		
		this.index_key_nokey[top1]=100;
		top1++;
		
		while(!this.tablename_list.isEmpty())
    	{
			n1=n0;
			search_position=-1000;
			Cell c1=new Cell(this.tablename_list.get(this.tablename_list.size()-1));
			this.tablename_list.remove(this.tablename_list.size()-1);
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
	    				n1=new Node(n1.sonNodeList.get(i),db.cacheManager,tran);
	    			}
	    			else {
	    				break;
	    			}
	    		}
	    	}
	    	
	    	if(search_position!=-1000)
	    	{
	    		root_node_table.add(new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()), db.cacheManager, tran));
		    	
		    	Node_index ni1;
				
				List<Node_itree> nit2=new ArrayList<Node_itree>();
				
				if(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s())!=-1)
		    	{
		    		ni1=new Node_index(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s()),db.cacheManager,tran);
		    	
			    	while(true)
			    	{
			    		for(i=0;i<ni1.indexpages.size();i++)
			    		{
			    			nit2.add(new Node_itree(ni1.indexpages.get(i),db.cacheManager,tran));
			    		}
			    		if(i<20||ni1.sonpage==-1)
			    		{
			    			break;
			    		}
			    		else {
			    			ni1=new Node_index(ni1.sonpage,db.cacheManager,tran);
			    		}
			    	}
		    	}
				
				nit3.add(nit2);
	    	}
    	}
		
		if(root_node_table.size()==0)
		{
			return;
		}
    	
		if(db.cacheManager.search_result_final!=null)
		{
			db.cacheManager.search_result_final.clear();
		}
		else {
			db.cacheManager.search_result_final=new ArrayList<String>();
		}
    	
    	List<String> whole_condition=new ArrayList<String>();
    	
    	for(String s1:this.where_condition)
    	{
    		whole_condition.add(s1);
    	}
    	
    	this.is_whole_not_null=true;
    	
    	for(i=0;i<whole_condition.size();)
    	{
    		this.where_condition.clear();
    		for(j=i;j<whole_condition.size();j++)
    		{
    			if(whole_condition.get(j)==null)
    			{
    				this.where_condition.add(whole_condition.get(j));
    			}
    		    else if(!whole_condition.get(j).equalsIgnoreCase("Or"))
    			{
    				if(!whole_condition.get(j).equalsIgnoreCase("and"))
    				{
    					this.where_condition.add(whole_condition.get(j));
    				}
    			}
    			else {
    				this.where_condition.add(null);
    				j=j+2;
    				i=j;
    				break;
    			}
    		}
    		
    		select_auto(root_node_table,nit3);
    		
    		time=time+this.end-this.start;
    		
    		this.index_key_nokey[top1]=100;
    		top1++;
    	
    	}
    	
    	if(whole_condition.size()==0)
    	{
    		this.is_whole_not_null=false;
    		select_auto(root_node_table,nit3);
    		
    		time=time+this.end-this.start;
    		
    		this.index_key_nokey[top1]=100;
    		top1++;
    		
    	}
    	
    	String s1;
    	result1=db.cacheManager.search_result_final;
    	int flag1;
    	
    	while(result1.size()!=0)
    	{
    		s1=result1.get(0);
    		result1.remove(0);
    		for(j=0;j<result1.size();j++)
    		{
    			if(s1.contentEquals(result1.get(j)))
				{
					result1.remove(j);
					j--;
				}
    		}
    		
    		
    		flag1=1;
    		for(i=0;i<s1.length();)
    		{
    			
    			if(s1.charAt(i)==' ')
    			{
    				i++;
    			}
    			
    			else if(s1.charAt(i)=='n')
    			{
    				if(i+3<s1.length())
    				{
    					if(s1.charAt(i+1)!='u')
    					{
    						flag1=0;
    						break;
    					}
    					else if(s1.charAt(i+2)!='l')
    					{
    						flag1=0;
    						break;
    					}
    					else if(s1.charAt(i+3)!='l')
    					{
    						flag1=0;
    						break;
    					}
    				}
    				
    				else {
    					flag1=0;
    					break;
    				}
    				i=i+4;
    			}
    			
    			else {
    				flag1=0;
    				break;
    			}
    			
    		}
    		
    		if(flag1==0)
    		{
    			result2.add(new String(s1));
    		}
    	}
    	
    	System.out.println();
    	
    	if(result2.size()!=0)
    	{
    		
    		System.out.println("输出相应的查找值");
    		
    		for(i=0;i<result2.size();i++)
	    	{
	    		System.out.println(result2.get(i));
	    	}
    		
    		System.out.println("输出相应的查找值输出完");
    	}
    	
    	else {
    		System.out.println("查找的相应值不在表格中");
    	}
    	
    	System.out.println();
    	
    	System.out.println("总查找时间（不包括打印时间）："+time+"ms");
    	
    	for(i=0,j=1/*第几个组*/,k=0/*第几个主键*/,l=0/*第几个索引*/;i<this.top1-1;i++)
    	{
    		
    		if(this.index_key_nokey[i]==100&&i!=this.top1-1)
    		{
    			System.out.println("查找第"+j+"组，用了以下：");
    			j++;
    		}
    		
    	    else if(this.index_key_nokey[i]==0)
    		{
    			System.out.println("索引查找，具体表为："+this.key_names.get(k));
    			k++;
    			
    			System.out.println("具体键为：");
    			
    			for(int m=0;m<this.index_names.get(l).size();m++)
    			{
    				System.out.println(this.index_names.get(l).get(m));
    			}
    			System.out.println();
    			l++;
    		}
    		
    		else if(this.index_key_nokey[i]==1)
    		{
    			System.out.println("主键查找，具体表为："+this.key_names.get(k));
    			k++;
    			
    			System.out.println("具体键为：");
    			
    			System.out.println(this.key_names.get(k));
    			k++;
    			
    			System.out.println();
    		}
    		
    		else {
    			System.out.println("没有用键查找，具体表为："+this.key_names.get(k));
    			k++;
    			System.out.println();
    		}
    	}
    	
    	System.out.println();
    	
    	result1.clear();
    	result2.clear();
    }
	
	private void select_auto(List<Node> root_node_table,List<List<Node_itree>> nit3) throws IOException, ClassNotFoundException {
		int i,j,k,x,y;
		int flag1;
		int itree1=-1000;
		List<Integer> select_column_table=new ArrayList<Integer>();//记录每个select后的列名是属于where_belong的哪一个表的
																   //第0个对应select的第0个列名，第0个的值对应where_belong的第几个表
																   //如果选择的是*就没有用，什么都不记录
		List<List<String>> where_belong=new ArrayList<List<String>>();//每个单独表的所有列条件都存在这里面，第一个元素代表where所有
																	  //root_node_table中第一个元素的表格相对应的列条件	

		
		/////
		
		for(i=0;i<this.selectedColumns.size();i++)
		{
			select_column_table.add(null);
		}
		
		if(root_node_table.size()>1&&this.is_whole_not_null)
		{
			
			this.start=System.currentTimeMillis();
			
			where_belong.clear();
			
			y=0;
			for(Node n2:root_node_table)
			{
				List<String> ls=new ArrayList<String>();
				where_belong.add(ls);
				for(i=0;i<n2.columnname.size();i++)
				{
					String s1=n2.columnname.get(i);
					for(j=0;j<where_condition.size();j=j+4)
					{
						if(s1.contentEquals(where_condition.get(j)))
						{
							ls.add(String.valueOf(i));//存储列号
							ls.add(where_condition.get(j));
							ls.add(where_condition.get(j+1));
							ls.add(where_condition.get(j+2));
							ls.add(where_condition.get(j+3));
						}
					}
					for(j=0;j<this.selectedColumns.size();j++)
					{
						if(s1.contentEquals(this.selectedColumns.get(j)))
						{
							select_column_table.set(j,y);
						}
					}
				}
				y++;
			}
			
			if(db.cacheManager.search_result!=null)
			{
				db.cacheManager.search_result.clear();
			}
			
			else {
				db.cacheManager.search_result=new ArrayList<Row>();
			}
			
			for(i=0;i<root_node_table.size();i++)//第一个表的搜索结果和第二个表的搜索结果用null分隔
			{
				Node n2=root_node_table.get(i);
				
				List<Node_itree>nit2=nit3.get(i);
				
				
				if(where_belong.get(i).size()==0)
				{
					n2.get_node_all(n2, tran);
					
					this.index_key_nokey[top1]=1;
					top1++;
					
					
					this.key_names.add(n2.node_tablename);
					this.key_names.add(n2.keyName);
					
					db.cacheManager.search_result.add(null);
					continue;
				}
				
				
				flag1=0;
				
				for(int i2=0;i2<where_belong.get(i).size();i2=i2+5)//把主键比较条件调到第一个
				{
					if(where_belong.get(i).get(i2+1).contentEquals(n2.keyName))
					{
						
						this.key_names.add(n2.node_tablename);
						this.key_names.add(n2.keyName);
						
						flag1=1;
						
						String []s3=new String[5];
						s3[0]=where_belong.get(i).get(i2);
						s3[1]=where_belong.get(i).get(i2+1);
						s3[2]=where_belong.get(i).get(i2+2);
						s3[3]=where_belong.get(i).get(i2+3);
						s3[4]=where_belong.get(i).get(i2+4);
						
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						
						where_belong.get(i).add(0,s3[0]);
						where_belong.get(i).add(1,s3[1]);
						where_belong.get(i).add(2,s3[2]);
						where_belong.get(i).add(3,s3[3]);
						where_belong.get(i).add(4,s3[4]);
						
						s3=null;
						break;
					}
					
					else{
						for(int i3=0;i3<nit2.size();i3++)
						{
							if(where_belong.get(i).get(i2+1).contentEquals(nit2.get(i3).keyname.get(0)))
							{
								this.key_names.add(nit2.get(i3).tablename);
								this.index_names.add(nit2.get(i3).keyname);
								flag1=2;
								itree1=i3;
								break;
							}
						}
						if(flag1==2)
						{
							String []s3=new String[5];
							s3[0]=where_belong.get(i).get(i2);
							s3[1]=where_belong.get(i).get(i2+1);
							s3[2]=where_belong.get(i).get(i2+2);
							s3[3]=where_belong.get(i).get(i2+3);
							s3[4]=where_belong.get(i).get(i2+4);
							
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							
							where_belong.get(i).add(0,s3[0]);
							where_belong.get(i).add(1,s3[1]);
							where_belong.get(i).add(2,s3[2]);
							where_belong.get(i).add(3,s3[3]);
							where_belong.get(i).add(4,s3[4]);
							
							s3=null;
							break;
						}
					}
				}
				if(flag1==2)
				{
					this.index_key_nokey[top1]=0;
					top1++;			
					
					switch (where_belong.get(i).get(3).toUpperCase())
					{
						case "EQ":
							nit2.get(itree1).search_equal_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
		
						case "GT":
							nit2.get(itree1).search_greater_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "GE":
							nit2.get(itree1).search_greater_or_equal_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LT":
							nit2.get(itree1).search_letter_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LE":
							nit2.get(itree1).search_letter_or_equal_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
					}
					
				}
				
				else if(flag1==1)
				{
					this.index_key_nokey[top1]=1;
					top1++;
					
					switch (where_belong.get(i).get(3).toUpperCase())
					{
						case "EQ":
							n2.search_equal_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
		
						case "GT":
							n2.search_greater_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "GE":
							n2.search_greater_or_equal_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LT":
							n2.search_letter_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LE":
							n2.search_letter_or_equal_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
					}
				}
				
				else if(flag1==0)
				{
					this.index_key_nokey[top1]=2;
					top1++;
					
					this.key_names.add(n2.node_tablename);
					
					switch (where_belong.get(i).get(3).toUpperCase())
					{
						case "EQ":
							n2.search_equal_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
		
						case "GT":
							n2.search_greater_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "GE":
							n2.search_greater_or_equal_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LT":
							n2.search_letter_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LE":
							n2.search_letter_or_equal_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
					}
					
				}
				
				db.cacheManager.search_result.add(null);
				
				
				for(j=db.cacheManager.search_result.size()-2;j>0;j--)
				{
					if(db.cacheManager.search_result.get(j)==null)
					{
						j++;
						break;
					}
				}
				
				if(j>=0)
				{
					List<String>l2=where_belong.get(i);
					for(k=5;k<l2.size();k=k+5)
					{
						for(x=j;x<db.cacheManager.search_result.size()-1/*因为最后一个是null*/;x++)
						{
							switch (l2.get(k+3).toUpperCase())
							{
								case "EQ":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!s3.contentEquals(l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "GT":
								{	
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.bigerThan(s3, l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "GE":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.bigerThan(s3, l2.get(k+2))&&!s3.contentEquals(l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "LT":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.letterThan(s3, l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "LE":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.letterThan(s3, l2.get(k+2))&&!s3.contentEquals(l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
							}
						}
					}
				}
				else {
					//在第i（i从0开始）个表中没有搜到结果
				}
			}
			this.end=System.currentTimeMillis();
		}
		
		else if(root_node_table.size()==1&&this.is_whole_not_null)
		{
			
			this.start=System.currentTimeMillis();
			
			List<String> ls=new ArrayList<String>();
			where_belong.add(ls);
			Node n2=root_node_table.get(0);
			for(i=0;i<n2.columnname.size();i++)
			{
				String s1=n2.columnname.get(i);
				for(j=0;j<where_condition.size();j=j+4)
				{
					if(s1.contentEquals(where_condition.get(j)))
					{
						ls.add(String.valueOf(i));
						ls.add(where_condition.get(j));
						ls.add(where_condition.get(j+1));
						ls.add(where_condition.get(j+2));
						ls.add(where_condition.get(j+3));
					}
				}
			}
			
			for(y=0;y<this.selectedColumns.size();y++)//就只有一个表
			{
				select_column_table.set(y, 0);
			}
			
			if(db.cacheManager.search_result!=null)
			{
				db.cacheManager.search_result.clear();
			}
			
			else {
				db.cacheManager.search_result=new ArrayList<Row>();
			}
			
			for(i=0;i<root_node_table.size();i++)//第一个表的搜索结果和第二个表的搜索结果用null分隔
			{
				n2=root_node_table.get(i);
				
				List<Node_itree>nit2=nit3.get(i);
				
				if(where_belong.get(i).size()==0)
				{
					n2.get_node_all(n2, tran);
					
					this.index_key_nokey[top1]=1;
					top1++;
					
					
					this.key_names.add(n2.node_tablename);
					this.key_names.add(n2.keyName);
					
					db.cacheManager.search_result.add(null);
					
					continue;
				}
				
				flag1=0;
				
				for(int i2=0;i2<where_belong.get(i).size();i2=i2+5)//把主键比较条件调到第一个
				{
					if(where_belong.get(i).get(i2+1).contentEquals(n2.keyName))
					{
						this.key_names.add(n2.node_tablename);
						this.key_names.add(n2.keyName);
						
						flag1=1;
						
						String []s3=new String[5];
						s3[0]=where_belong.get(i).get(i2);
						s3[1]=where_belong.get(i).get(i2+1);
						s3[2]=where_belong.get(i).get(i2+2);
						s3[3]=where_belong.get(i).get(i2+3);
						s3[4]=where_belong.get(i).get(i2+4);
						
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						where_belong.get(i).remove(i2);
						
						where_belong.get(i).add(0,s3[0]);
						where_belong.get(i).add(1,s3[1]);
						where_belong.get(i).add(2,s3[2]);
						where_belong.get(i).add(3,s3[3]);
						where_belong.get(i).add(4,s3[4]);
						
						s3=null;
						break;
					}
					
					else{
						for(int i3=0;i3<nit2.size();i3++)
						{
							if(where_belong.get(i).get(i2+1).contentEquals(nit2.get(i3).keyname.get(0)))
							{
								this.key_names.add(nit2.get(i3).tablename);
								this.index_names.add(nit2.get(i3).keyname);
								
								flag1=2;
								itree1=i3;
								break;
							}
						}
						if(flag1==2)
						{
							String []s3=new String[5];
							s3[0]=where_belong.get(i).get(i2);
							s3[1]=where_belong.get(i).get(i2+1);
							s3[2]=where_belong.get(i).get(i2+2);
							s3[3]=where_belong.get(i).get(i2+3);
							s3[4]=where_belong.get(i).get(i2+4);
							
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							where_belong.get(i).remove(i2);
							
							where_belong.get(i).add(0,s3[0]);
							where_belong.get(i).add(1,s3[1]);
							where_belong.get(i).add(2,s3[2]);
							where_belong.get(i).add(3,s3[3]);
							where_belong.get(i).add(4,s3[4]);
							
							s3=null;
							break;
						}
					}
				}
				
				if(flag1==2)
				{
					this.index_key_nokey[top1]=0;
					top1++;
					
					
					switch (where_belong.get(i).get(3).toUpperCase())
					{
						case "EQ":
							nit2.get(itree1).search_equal_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
		
						case "GT":
							nit2.get(itree1).search_greater_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "GE":
							nit2.get(itree1).search_greater_or_equal_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LT":
							nit2.get(itree1).search_letter_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LE":
							nit2.get(itree1).search_letter_or_equal_all(nit2.get(itree1), new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
					}
					
				}
				
				else if(flag1==1)
				{
					this.index_key_nokey[top1]=1;
					top1++;
					
					
					switch (where_belong.get(i).get(3).toUpperCase())
					{
						case "EQ":
							n2.search_equal_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
		
						case "GT":
							n2.search_greater_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "GE":
							n2.search_greater_or_equal_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LT":
							n2.search_letter_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LE":
							n2.search_letter_or_equal_all(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
					}
					
				}
				
				else if(flag1==0)
				{
					this.index_key_nokey[top1]=2;
					top1++;
					
					this.key_names.add(n2.node_tablename);
					
					switch (where_belong.get(i).get(3).toUpperCase())
					{
						case "EQ":
							n2.search_equal_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
		
						case "GT":
							n2.search_greater_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "GE":
							n2.search_greater_or_equal_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LT":
							n2.search_letter_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
							
						case "LE":
							n2.search_letter_or_equal_all_nokey(n2, new Cell(where_belong.get(i).get(2)), Integer.parseInt(where_belong.get(i).get(0)), tran);
							break;
					}

				}
				
				db.cacheManager.search_result.add(null);
				
				
				for(j=db.cacheManager.search_result.size()-2;j>0;j--)
				{
					if(db.cacheManager.search_result.get(j)==null)
					{
						j++;
						break;
					}
				}
				
				if(j>=0)
				{
					List<String>l2=where_belong.get(i);
					for(k=5;k<l2.size();k=k+5)
					{
						for(x=j;x<db.cacheManager.search_result.size()-1/*因为最后一个是null*/;x++)
						{
							switch (l2.get(k+3).toUpperCase())
							{
								case "EQ":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!s3.contentEquals(l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "GT":
								{	
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.bigerThan(s3, l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "GE":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.bigerThan(s3, l2.get(k+2))&&!s3.contentEquals(l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "LT":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.letterThan(s3, l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
								
								case "LE":
								{
									String s3=db.cacheManager.search_result.get(x).getCell(Integer.parseInt(l2.get(k))).getValue_s();
									if(!this.letterThan(s3, l2.get(k+2))&&!s3.contentEquals(l2.get(k+2)))
									{
										db.cacheManager.search_result.remove(x);
										x--;
									}
									break;
								}
							}
						}
					}
				}
				else {
					//在第i（i从0开始）个表中没有搜到结果
				}
			}
			
			this.end=System.currentTimeMillis();
			
		}
		
		else if(!this.is_whole_not_null)
		{
			this.start=System.currentTimeMillis();
			
			y=0;
			for(Node n2:root_node_table)
			{
				for(i=0;i<n2.columnname.size();i++)
				{
					
					String s1=n2.columnname.get(i);

					for(j=0;j<this.selectedColumns.size();j++)
					{
						if(s1.contentEquals(this.selectedColumns.get(j)))
						{
							select_column_table.set(j,y);
						}
					}
				}
				y++;
			}
			
			if(db.cacheManager.search_result!=null)
			{
				db.cacheManager.search_result.clear();
			}
			
			else {
				db.cacheManager.search_result=new ArrayList<Row>();
			}
			
			for(int i4=0;i4<root_node_table.size();i4++)
			{
				root_node_table.get(i4).get_node_all(root_node_table.get(i4), tran);
				db.cacheManager.search_result.add(null);
				
				this.index_key_nokey[top1]=1;
				this.top1++;
				
				this.key_names.add(root_node_table.get(i4).node_tablename);
				this.key_names.add(root_node_table.get(i4).keyName);
				
			}	
			
			this.end=System.currentTimeMillis();
		}
		
		/////
		
		List<List<String>> l1=new ArrayList<List<String>>();
		
		if(!this.selectedColumns.get(0).contentEquals("*"))
		{
			y=0;
			for(Integer tp:select_column_table)
			{
				for(i=0,j=0;i<tp&&j<db.cacheManager.search_result.size();/**/)
				{
					while(db.cacheManager.search_result.get(j)!=null&&j<db.cacheManager.search_result.size())
					{
						j++;
					}
					i++;
					j++;
				}
				List<String> l2=new ArrayList<String>();
				Node n2=root_node_table.get(tp);
				
				for(k=0;k<n2.columnname.size();k++)
				{
					if(n2.columnname.get(k).contentEquals(this.selectedColumns.get(y)))
					{
						break;
					}
				}
				
				if(db.cacheManager.search_result.get(j)!=null)
				{
					for(;db.cacheManager.search_result.get(j)!=null;j++)
					{
						l2.add(db.cacheManager.search_result.get(j).getCell(k).getValue_s());
					}
				}
				else {
					l2.add("null");
				}
				
				l1.add(l2);
				y++;
			}
			
			class c1{
				public Integer i1;//当前列串已经处理到哪个String
				public Integer i2;//当前列串在外面的不是类里面的l1中是哪一个，第一个编号是0
				public List<String> l1;
				public c1(Integer i,Integer ii, List<String> l)
				{
					i1=i;
					i2=ii;
					l1=l;
				}
			}
			
			Stack<c1> st1=new Stack<c1>();

			String s1="";
			c1 c1;
			
			st1.push(new c1(0,0,l1.get(0)));//l1中的第0个处理到了第0个String
			
			while(!st1.empty())
			{
				c1=st1.pop();
				if(c1.i1<c1.l1.size())
				{
					if(c1.i1!=0)
					{
						s1=s1.substring(0,s1.length()-c1.l1.get(c1.i1-1).length()-1);
					}
					s1=s1.concat(c1.l1.get(c1.i1)+" ");
					st1.push(new c1(c1.i1+1,c1.i2,c1.l1));
					if(c1.i2<l1.size()-1)
					{
						st1.push(new c1(0,c1.i2+1,l1.get(c1.i2+1)));
					}
					else {
						db.cacheManager.search_result_final.add(s1);
					}
				}
				else {
					s1=s1.substring(0,s1.length()-c1.l1.get(c1.i1-1).length()-1);
				}
			}
		}
		
		else {
			Integer [] position=new Integer[100];//指向某个表在search中的开始非null位置
			
			for(i=0;i<root_node_table.size();i++)
			{
				for(k=0,j=0;k<i&&j<db.cacheManager.search_result.size();)
				{
					for(;db.cacheManager.search_result.get(j)!=null&&j<db.cacheManager.search_result.size();j++)
					{
						
					}
					k++;
					j++;
				}
				position[i]=j;
			}
			
			position[i]=db.cacheManager.search_result.size();
			
			class c1{
				public Integer i1;//当前列串已经处理到哪个Row
								  //注意不是具体的位置，具体的位置是position[i2]+i1;
				public Integer i2;//当前列串在db的cachemager的search_result中的第几个表
								  //具体位置由position给出
				public c1(Integer i,Integer ii)
				{
					i1=i;
					i2=ii;
				}
			}
			
			Stack<c1> st1=new Stack<c1>();

			String s1="";
			c1 c1;
			
			st1.push(new c1(0,0));//第0个表处理到了第0个String
			
			while(!st1.empty())
			{
				c1=st1.pop();
				if(position[c1.i2]==position[c1.i2+1]-1)
				{
					if(c1.i1!=0)
					{
						for(i=0;i<root_node_table.get(c1.i2).columnname.size();i++)
						{
							s1=s1.substring(0,s1.length()-"null ".length());
						}
					}

					else {
						for(i=0;i<root_node_table.get(c1.i2).columnname.size();i++)
						{
							s1=s1.concat("null ");
						}
						
						st1.push(new c1(c1.i1+1,c1.i2));
					
						if(c1.i2<root_node_table.size()-1)
						{
							st1.push(new c1(0,c1.i2+1));
						}
						
						else {
							db.cacheManager.search_result_final.add(s1);
						}
					}
					
				}
				else if(c1.i1+position[c1.i2]<position[c1.i2+1]-1)
				{
					if(c1.i1!=0)
					{
						for(i=0;i<db.cacheManager.search_result.get(c1.i1+position[c1.i2]-1).cellList.size();i++)
						{
							s1=s1.substring(0,s1.length()-db.cacheManager.search_result.get(c1.i1+position[c1.i2]-1).getCell(i).getValue_s().length()-1);
						}
					}
					
					for(i=0;i<db.cacheManager.search_result.get(c1.i1+position[c1.i2]).cellList.size();i++)
					{
						s1=s1.concat(db.cacheManager.search_result.get(c1.i1+position[c1.i2]).getCell(i).getValue_s()+" ");
					}

					st1.push(new c1(c1.i1+1,c1.i2));
					
					if(c1.i2<root_node_table.size()-1)
					{
						st1.push(new c1(0,c1.i2+1));
					}
					
					else {
						db.cacheManager.search_result_final.add(s1);
					}
				}
				else {
					for(i=0;i<db.cacheManager.search_result.get(c1.i1+position[c1.i2]-1).cellList.size();i++)
					{
						s1=s1.substring(0,s1.length()-db.cacheManager.search_result.get(c1.i1+position[c1.i2]-1).getCell(i).getValue_s().length()-1);
					}
				}
			}
		}
		
//		for(i=0;i<db.cacheManager.search_result_final.size();i++)
//		{
//			System.out.println(db.cacheManager.search_result_final.get(i));
//		}
	}

	private void delete() throws IOException, ClassNotFoundException 
	{
		int i,j;
		Integer search_position=-1000;
		if (!isUserTransaction) 
		{
			tran = db.beginWriteTrans();
		}
		
		Node n1=new Node(-2,db.cacheManager,tran);
		List<Node> root_node_table=new ArrayList<Node>();
		while(!this.tablename_list.isEmpty())//单表
    	{
			search_position=-1000;
			Cell c1=new Cell(this.tablename_list.get(this.tablename_list.size()-1));
			this.tablename_list.remove(this.tablename_list.size()-1);
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
	    				n1=new Node(n1.sonNodeList.get(i),db.cacheManager,tran);
	    			}
	    			else {
	    				break;
	    			}
	    		}
	    	}
	    	root_node_table.add(new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()), db.cacheManager, tran));
    	}
		
		Node_index ni1;
		
		List<Node_itree> nit2=new ArrayList<Node_itree>();
		
		if(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s())!=-1)
    	{
    		ni1=new Node_index(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s()),db.cacheManager,tran);
    	
	    	while(true)
	    	{
	    		for(i=0;i<ni1.indexpages.size();i++)
	    		{
	    			nit2.add(new Node_itree(ni1.indexpages.get(i),db.cacheManager,tran));
	    		}
	    		if(i<20||ni1.sonpage==-1)
	    		{
	    			break;
	    		}
	    		else {
	    			ni1=new Node_index(ni1.sonpage,db.cacheManager,tran);
	    		}
	    	}
    	}
		
    	List<String> whole_condition=new ArrayList<String>();
    	
    	if(this.where_condition.size()!=0)
    	{
    		for(String s1:this.where_condition)
	    	{

    			whole_condition.add(s1);
    				
	    	}
	    	for(i=0;i<whole_condition.size();)
	    	{
	    		this.where_condition.clear();
	    		for(j=i;j<whole_condition.size();j++)
	    		{
	    			if(whole_condition.get(j)==null)
	    			{
	    				this.where_condition.add(whole_condition.get(j));
	    			}
	    		    else if(!whole_condition.get(j).equalsIgnoreCase("Or"))
	    			{
	    				if(!whole_condition.get(j).equalsIgnoreCase("and"))
	    				{
	    					this.where_condition.add(whole_condition.get(j));
	    				}
	    			}
	    			else {
	    				this.where_condition.add(null);
	    				j=j+2;
	    				i=j;
	    				break;
	    			}
	    		}
	    		
	    		delete_auto(root_node_table.get(0),nit2);
	    	
	    	}
	    	
    	}
    	else {
    		Node n3;
    		Node_itree nit3;
    		Stack<Node>st1=new Stack<Node>();//把所有空页挂在空闲页表上，
    		Stack<Node_itree>st2=new Stack<Node_itree>();
    	
    		n3=root_node_table.get(0);
			for(i=0;i<n3.sonNodeList.size();i++)
			{
				st1.push(new Node(n3.sonNodeList.get(i),db.cacheManager,tran));
			}
			
			n3.sonNodeList.clear();
			n3.rowList.clear();
			n3.fatherNodeID=-1;
			n3.rhizine=n3;
			n3.intoBytes(tran);
			
    		while(!st1.empty())
    		{
    			n3=st1.pop();
    			for(i=0;i<n3.sonNodeList.size();i++)
    			{
    				st1.push(new Node(n3.sonNodeList.get(i),db.cacheManager,tran));
    			}
    			db.cacheManager.unusedList_PageID.add(n3.pageOne);
    		}
    		
    		for(i=0;i<nit2.size();i++)
    		{
    			
    			nit3=nit2.get(i);
    			for(int k=0;k<nit3.sonNodeList.size();k++)
    			{
    				st2.push(new Node_itree(nit3.sonNodeList.get(k),db.cacheManager,tran));
    			}
    			
    			nit3.rowList.clear();
    			nit3.sonNodeList.clear();
    			nit3.fatherNodeID=-1;
    			nit3.rhizine=nit3;
    			nit3.intoBytes(tran);
    			
    			while(!st2.empty())
    			{
    				nit3=st2.pop();
        			for(int l=0;l<nit3.sonNodeList.size();l++)
        			{
        				st2.push(new Node_itree(nit3.sonNodeList.get(l),db.cacheManager,tran));
        			}
        			db.cacheManager.unusedList_PageID.add(nit3.pageOne);
    			}
    		}
    		
    	}
	}

	public void delete_auto(Node root_node_table,List<Node_itree> nit2) throws ClassNotFoundException, IOException
	{
		int i,j;
		List<Cell> keys=new ArrayList<Cell>();
		List<Integer> keyps=new ArrayList<Integer>();
		List<String> condition=new ArrayList<String>();
		for(i=0;i<this.where_condition.size();i=i+4)
		{
			for(j=0;j<root_node_table.columnname.size();j++)
			{
				if(root_node_table.columnname.get(j).contentEquals(this.where_condition.get(i)))
				{
					keys.add(new Cell(this.where_condition.get(i+1)));//具体的值
					keyps.add(j);//列号
					condition.add(this.where_condition.get(i+2));//大于小于等于等
				}
			}
		}
		root_node_table.delete_all_nokey(root_node_table, keys,keyps, condition,tran);
		
		for(i=0;i<nit2.size();i++)
		{
			nit2.get(i).delete_all_nokey(nit2.get(i), keys, keyps, condition, tran);
		}
	}
	
	private Node delete_update() throws IOException, ClassNotFoundException 
	{
		int i,j;
		Integer search_position=-1000;
		if (!isUserTransaction) 
		{
			tran = db.beginWriteTrans();
		}
		
		Node n1=new Node(-2,db.cacheManager,tran);
		List<Node> root_node_table=new ArrayList<Node>();
		while(!this.tablename_list.isEmpty())
    	{
			search_position=-1000;
			Cell c1=new Cell(this.tablename_list.get(this.tablename_list.size()-1));
			this.tablename_list.remove(this.tablename_list.size()-1);
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
	    				n1=new Node(n1.sonNodeList.get(i),db.cacheManager,tran);
	    			}
	    			else {
	    				break;
	    			}
	    		}
	    	}
	    	root_node_table.add(new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()), db.cacheManager, tran));
    	}
		
		
		Node_index ni1;
		
		List<Node_itree> nit2=new ArrayList<Node_itree>();
		
		if(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s())!=-1)
    	{
    		ni1=new Node_index(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s()),db.cacheManager,tran);
    	
	    	while(true)
	    	{
	    		for(i=0;i<ni1.indexpages.size();i++)
	    		{
	    			nit2.add(new Node_itree(ni1.indexpages.get(i),db.cacheManager,tran));
	    		}
	    		if(i<20||ni1.sonpage==-1)
	    		{
	    			break;
	    		}
	    		else {
	    			ni1=new Node_index(ni1.sonpage,db.cacheManager,tran);
	    		}
	    	}
    	}
		
    	List<String> whole_condition=new ArrayList<String>();
    	
    	if(this.where_condition.size()!=0)
    	{
    		for(String s1:this.where_condition)
	    	{
	    		whole_condition.add(s1);
	    	}
	    	for(i=0;i<whole_condition.size();)
	    	{
	    		this.where_condition.clear();
	    		for(j=i;j<whole_condition.size();j++)
	    		{
	    			if(whole_condition.get(j)==null)
	    			{
	    				this.where_condition.add(whole_condition.get(j));
	    			}
	    		    else if(!whole_condition.get(j).equalsIgnoreCase("Or"))
	    			{
	    				if(!whole_condition.get(j).equalsIgnoreCase("and"))
	    				{
	    					this.where_condition.add(whole_condition.get(j));
	    				}
	    			}
	    			else {
	    				this.where_condition.add(null);
	    				j=j+2;
	    				i=j;
	    				break;
	    			}
	    		}
	    		
	    		delete_auto_update(root_node_table.get(0),nit2);
	    		
	    	}
    	}
    	else {
    		root_node_table.get(0).delete_zheng_ge_shu_bao_cun_geng_ye(root_node_table.get(0), tran);//把所有空页挂在空闲页表上，现在还不行
    		for(i=0;i<nit2.size();i++)
    		{
    			nit2.get(i).delete_zheng_ge_shu_bao_cun_geng_ye(nit2.get(i), tran);
    		}
    	}
    	
    	this.nit2=nit2;
    	
    	return root_node_table.get(0);
	}

	public void delete_auto_update(Node root_node_table,List<Node_itree> nit2) throws ClassNotFoundException, IOException
	{
		int i,j;
		List<Cell> keys=new ArrayList<Cell>();
		List<Integer> keyps=new ArrayList<Integer>();
		List<String> condition=new ArrayList<String>();
		for(i=0;i<this.where_condition.size();i=i+4)
		{
			for(j=0;j<root_node_table.columnname.size();j++)
			{
				if(root_node_table.columnname.get(j).contentEquals(this.where_condition.get(i)))
				{
					keys.add(new Cell(this.where_condition.get(i+1)));
					keyps.add(j);
					condition.add(this.where_condition.get(i+2));
				}
			}
		}
		root_node_table.delete_all_nokey2(root_node_table, keys, keyps, condition,tran);
		for(i=0;i<nit2.size();i++)
		{
			nit2.get(i).delete_all_nokey(nit2.get(i), keys, keyps, condition, tran);
		}
	}
	
	private void update() throws IOException, ClassNotFoundException 
	{
		Node root_node_table;
		if (!isUserTransaction) 
		{
			tran = db.beginWriteTrans();
		}
		
		db.cacheManager.delete_update.clear();
		
		root_node_table=delete_update();
		
		int i,j;
		List<String> keys=new ArrayList<String>();
		List<Integer> keyps=new ArrayList<Integer>();
		for(i=0;i<this.update_condition.size();i=i+3)
		{
			for(j=0;j<root_node_table.columnname.size();j++)
			{
				if(root_node_table.columnname.get(j).contentEquals(this.update_condition.get(i)))
				{
					keys.add(this.update_condition.get(i+1));
					keyps.add(j);
				}
			}
		}
		
		Row r1;
		for(i=0;i<db.cacheManager.delete_update.size();i++)
		{
			r1=db.cacheManager.delete_update.get(i);
			for(j=0;j<keyps.size();j++)
			{
				r1.getCell(keyps.get(j)).setCell(keys.get(j));
			}
		}
		
		for(i=0;i<db.cacheManager.delete_update.size();i++)
		{
			root_node_table.insertRow(db.cacheManager.delete_update.get(i), tran);
			//由于rhizine的影响，只允许用根所在的节点进行insert操作（首先，之后这个节点也不是根节点了，但不存在问题）
			for(j=0;j<this.nit2.size();j++)
			{
				this.nit2.get(j).insertRow(db.cacheManager.delete_update.get(i), tran);
			}
			
		}
	}

	//由于rhizine的影响，只允许用根所在的节点进行insert操作（首先，之后这个节点也不是根节点了，但不存在问题）
	//由于rhizine的影响，只允许用根所在的节点进行insert操作
	//由于rhizine的影响，只允许用根所在的节点进行insert操作
	//由于rhizine的影响，只允许用根所在的节点进行insert操作
	//由于rhizine的影响，只允许用根所在的节点进行insert操作
	//由于rhizine的影响，只允许用根所在的节点进行insert操作
	private void insert() throws IOException, ClassNotFoundException 
	{
		Node root_node_table;
		Node_index ni1;
		List<Node_itree> nit2=new ArrayList<Node_itree>();
		if (!isUserTransaction) 
		{
			tran = db.beginWriteTrans();
		}
		
		int i,j;
		int search_position=-1000;
		Node n1=new Node(-2,db.cacheManager,tran);
    	Cell c1=new Cell(this.tablename_list.get(0));
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
    				n1=new Node(n1.sonNodeList.get(i),db.cacheManager,tran);
    			}
    			else {
    				break;
    			}
    		}
    	}
		
    	root_node_table=new Node(Integer.parseInt(n1.rowList.get(search_position).getCell(1).getValue_s()),db.cacheManager,tran);
    	
    	if(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s())!=-1)
    	{
    		ni1=new Node_index(Integer.parseInt(n1.rowList.get(search_position).getCell(2).getValue_s()),db.cacheManager,tran);
    	
	    	while(true)
	    	{
	    		for(i=0;i<ni1.indexpages.size();i++)
	    		{
	    			nit2.add(new Node_itree(ni1.indexpages.get(i),db.cacheManager,tran));
	    		}
	    		if(i<20||ni1.sonpage==-1)
	    		{
	    			break;
	    		}
	    		else {
	    			ni1=new Node_index(ni1.sonpage,db.cacheManager,tran);
	    		}
	    	}
    	}
    	
		//这是做的修改的部分

		//这是做的修改的部分
    	
    	List<String> s1=new ArrayList<String>();
    	
    	int flag2;
    	for(i=0;i<root_node_table.columnname.size();i++)
    	{
    		flag2=0;
    		for(j=0;j<this.insert_column_names.size();j++)
    		{
    			if(root_node_table.columnname.get(i).contentEquals(this.insert_column_names.get(j)))
    			{
    				flag2=1;
    				s1.add(this.insert_column_values.get(j));
    				this.insert_column_names.remove(j);
    				this.insert_column_values.remove(j);
    				break;
    			}
    		}
    		if(flag2==0)
    		{
    			s1.add("null");
    		}
    	}
		
    	Row r1=new Row(s1);
    	
		root_node_table.insertRow(r1, tran);
		
		for(i=0;i<nit2.size();i++)
		{
			nit2.get(i).insertRow(r1, tran);
		}
	}

	private static BasicType lowestType(String strVal) {
		int dot = 0;
		boolean alpha = false;
		for (int i = 0; i < strVal.length(); i++) {
			char c = strVal.charAt(i);
			if (c == '.') {
				dot++;
			} else if (c > '9' || c < '0') {
				alpha = true;
				break;
			}
		}
		if (alpha == true || dot >= 2) {
			return BasicType.String;
		} else if (dot == 1) {
			return BasicType.Float;
		} else {
			return BasicType.Integer;
		}
	}

	private UnionOperand eval(List<EvalDiscription> evalDiscriptions, Cursor p)
			throws IOException, ClassNotFoundException {
		Expression exp = new Expression();
		List<String> info = db.getTable(targetTable, tran).getColumnsName();

		for (int i = 0; i < evalDiscriptions.size(); i++) {
			if (evalDiscriptions.get(i).cmd == OpCode.Operand) {
				if (evalDiscriptions.get(i).col_name != null) {

					for (int j = 0; j < info.size(); j++) {
						if (info.get(j).equals(evalDiscriptions.get(i).col_name)) {
							exp.addOperand(new UnionOperand(p.getColumnType(info.get(j)), p.getData().get(j)));
						}
					}

				} else {
					String val = evalDiscriptions.get(i).constant;
					BasicType cType = lowestType(val);
					exp.addOperand(new UnionOperand(cType, val));
				}
			} else {
				exp.applyOperator(evalDiscriptions.get(i).cmd);
			}
		}
		return exp.getAns();
	}

	private UnionOperand eval(List<EvalDiscription> evalDiscriptions, int Index) {
		Expression exp = new Expression();
		List<String> infoJoin = joinResult.getHeaderString();

		for (int i = 0; i < evalDiscriptions.size(); i++) {
			if (evalDiscriptions.get(i).cmd == OpCode.Operand) {
				if (evalDiscriptions.get(i).col_name != null) {

					for (int j = 0; j < infoJoin.size(); j++) {
						if (infoJoin.get(j).equals(evalDiscriptions.get(i).col_name)) {
							// System.out.println(joinResult.getRes().get(Index).get(j));
							exp.addOperand(new UnionOperand(joinResult.getHeader().get(j).getColumnTypeBasic(),
									joinResult.getRes().get(Index).get(j)));
							// exp.addOperand(new UnionOperand(BasicType.String,
							// joinResult.getRes().get(Index).get(j)));
						}
					}

				} else {
					String val = evalDiscriptions.get(i).constant;
					BasicType cType = lowestType(val);
					exp.addOperand(new UnionOperand(cType, val));
				}
			} else {
				exp.applyOperator(evalDiscriptions.get(i).cmd);
			}
		}
		return exp.getAns();
	}

	private void join(String tableName) throws IOException, ClassNotFoundException {
		Table table = db.getTable(tableName, tran);
		List<Column> fromTreeHead = new ArrayList<>();
		

		table.getColumnsName().forEach(n -> fromTreeHead.add(new Column(n)));
		List<BasicType> types = table.getColumnsType();

		for (int i = 0; i < types.size(); ++i) 
		{
			fromTreeHead.get(i).ColumnType = types.get(i).toString();
		}

		Cursor cursor = db.getTable(tableName, tran).createCursor(tran);

		if (joinResult == null) 
		{
			joinResult = new QueryResult(fromTreeHead);
			while (cursor != null) 
			{
				List<String> fromTreeString = cursor.getData();
				joinResult.addRecord(fromTreeString);
				if (cursor.moveToNext(tran) == false) 
				{
					cursor = null;
				}
			}
			return;
		}

		List<Column> joinHead = joinResult.getHeader();
		int snglJoin = joinResult.getHeader().size();
		table.getColumnsName().forEach(n -> joinHead.add(new Column(n)));
		for (int ndx1 = snglJoin; ndx1 < snglJoin + types.size(); ++ndx1) 
		{
			joinHead.get(ndx1).ColumnType = types.get(ndx1 - snglJoin).toString();
		}



		QueryResult joinRes = new QueryResult(joinHead);

		for (int ndx1 = 0; ndx1 < joinResult.getRes().size(); ++ndx1) 
		{
			while (cursor != null) {
				List<String> snglRecord = new ArrayList<>();
				for (int arri = 0; arri < joinResult.getRes().get(ndx1).size(); ++arri) 
				{
					snglRecord.add(joinResult.getRes().get(ndx1).get(arri));
				}

				for (int ndx3 = 0; ndx3 < cursor.getData().size(); ++ndx3) 
				{
					snglRecord.add(cursor.getData().get(ndx3));
				}
				
				joinRes.addRecord(snglRecord);
				if (cursor.moveToNext(tran) == false) 
				{
					cursor = null;
				}
			}
			cursor = db.getTable(tableName, tran).createCursor(tran);

		}
		joinResult = joinRes;
	}

	public JoinMatch checkUnion(List<Column> head1, List<Column> head2) {
		List<Column> unionHead = new ArrayList<>();
		Map<Integer, Integer> unionUnder = new HashMap<>();

		head1.forEach(n -> unionHead.add(n));

		for (Column n : head2) {
			if (!head1.contains(n)) {
				unionHead.add(n);
			}
		}

		for (int i = 0; i < head1.size(); i++) {
			int locate = head2.indexOf(head1.get(i));
			if (locate != -1) {
				unionUnder.put(i, locate);
			}
		}

		return new JoinMatch(unionHead, unionUnder);
	}

	// 鏉╂瑤閲滈弬瑙勭《閸欘亞鏁ゆ禍搴㈢ゴ鐠囨洝鍤滈悞鎯扮箾閹恒儲鎼锋担婧匡拷锟�
	public QueryResult forTestJoin(JoinMatch joinMatch, QueryResult input1, QueryResult input2) {
		int matchCount = 0;
		QueryResult copy = new QueryResult(joinMatch.getJoinHead());
		List<List<String>> resList = input1.getRes();
		for (int i = 0; i < resList.size(); i++) {
			List<String> tempRes = resList.get(i);
			for (List<String> fromTreeString : input2.getRes()) {
				List<String> copyTreeString = new ArrayList<>();
				fromTreeString.forEach(n -> copyTreeString.add(n));
				Iterator iterator = joinMatch.getJoinUnder().keySet().iterator();
				matchCount = 0;

				while (iterator.hasNext()) {
					int nextKey = (Integer) iterator.next();
					int nextValue = joinMatch.getJoinUnder().get(nextKey);
					String s1 = tempRes.get(nextKey);
					String s2 = fromTreeString.get(nextValue);
					if (!s1.equals(s2)) {
						break;
					} else {
						matchCount++;
						copyTreeString.remove(nextValue);
					}
				}

				if (matchCount == joinMatch.getJoinUnder().size()) {
					List<String> line = new ArrayList<>();
					tempRes.forEach(n -> line.add(n));
					copyTreeString.forEach(n -> line.add(n));
					copy.getRes().add(line);
				}
			}
		}
		return copy;
	}

}