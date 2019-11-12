package npu.zunsql;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;
import npu.zunsql.cache.Transaction;
import npu.zunsql.codegen.CodeGenerator;
import npu.zunsql.sqlparser.Parser;
import npu.zunsql.sqlparser.ast.Relation;
import npu.zunsql.virenv.Instruction;
import npu.zunsql.virenv.QueryResult;
import npu.zunsql.virenv.VirtualMachine;
import npu.zunsql.treemng.Database;
import npu.zunsql.treemng.Node;
import npu.zunsql.treemng.Node_index;
import npu.zunsql.treemng.Node_itree;
import npu.zunsql.virenv.OpCode;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Statement;
import java.util.List;
import java.util.Stack;
import java.util.ArrayList;

public class DBInstance
{
	public int and_or;//0是and，1是or
	
	private Database db;
	private VirtualMachine vm;

	private DBInstance(Database db)
	{
		this.db = db;
		this.vm = new VirtualMachine(db);
	}

	public static DBInstance Open(String name, int M)
	{
		Database db = null;

		try {
			db = new Database(name,M);
		}
		catch(IOException ie)
		{
			ie.printStackTrace();
			System.exit(-1);
		}
		catch(ClassNotFoundException ce) 
		{
			ce.printStackTrace();
			System.exit(-1);
		}
		return new DBInstance(db);
	}
	
	public static DBInstance Open(String name) 
	{
		Database db = null;

		try {
			db = new Database(name);
		}
		catch(IOException ie)
		{
			ie.printStackTrace();
			System.exit(-1);
		}
		catch(ClassNotFoundException ce) 
		{
			ce.printStackTrace();
			System.exit(-1);
		}
		return new DBInstance(db);
	}

	List<Relation> statements = new ArrayList<Relation>();
	
	public int where(String s1)
	{
		int position;
		int position2;
		char [] c=" and ".toCharArray();
		char [] cc=" or ".toCharArray();
		for(int i=0;i<2;i++)
		{
			c[1]=(char)(c[1]+i*('A'-'a'));
			cc[1]=(char)(cc[1]+i*('A'-'a'));
			for(int j=0;j<2;j++)
			{
				c[2]=(char)(c[2]+j*('A'-'a'));
				cc[2]=(char)(cc[2]+j*('A'-'a'));
				for(int k=0;k<2;k++)
				{
					c[3]=(char)(c[3]+k*('A'-'a'));
					position=s1.indexOf(new String(c));
					position2=s1.indexOf(new String(cc));
					if(position!=-1&&position2!=-1)
					{
						if(position<position2)
						{
							and_or=0;
							return position;
						}
						else {
							and_or=1;
							return position2;
						}
					}
					
					else if(position!=-1)
					{
						and_or=0;
						return position;
					}
					
					else {
						and_or=1;
						return position2;
					}
				}
			}
		}
		return -1;
	}
	
	public int index(String s1)
	{
		int position;
		char [] c=" index ".toCharArray();
		for(int i=0;i<2;i++)
		{
			c[1]=(char)(c[1]+i*('A'-'a'));
			for(int j=0;j<2;j++)
			{
				c[2]=(char)(c[2]+j*('A'-'a'));
				for(int k=0;k<2;k++)
				{
					c[3]=(char)(c[3]+k*('A'-'a'));
					for(int l=0;l<2;l++)
					{
						c[4]=(char)(c[4]+k*('A'-'a'));
						for(int m=0;m<2;m++)
						{
							c[5]=(char)(c[5]+k*('A'-'a'));
							position=s1.indexOf(new String(c));
							if(position!=-1)
							{
								return position;
							}
						}
					}
				}
			}
		}
		return -1;
	}
	
	public static char[] cccc1="here ".toCharArray();
	public static char[] cccc2="and ".toCharArray();
	public static char[] cccc3="or ".toCharArray();
	
	
	public boolean where2(String s1)//看s1中是不是包含where
	{
		int flag;
		int i,j,k;
		for(i=0;i<s1.length();i++)
		{
			flag=1;
			if(i+1<s1.length()&&(s1.charAt(i)==' '&&(s1.charAt(i+1)=='w'||s1.charAt(i+1)=='W')))
			{
				for(j=i+2,k=0;k<5&&j<s1.length();j++,k++)
				{
					if(s1.charAt(j)!=cccc1[k]&&s1.charAt(j)!=cccc1[k]+'A'-'a')
					{
						flag=0;
						break;
					}
				}
				
				if(flag==1&&j<s1.length())
				{
					return true;
				}
			}
		}
		return false;
	}
	
	public int where3(String s1)//看s1中是不是包含where
	{
		int flag;
		int i,j,k;
		for(i=0;i<s1.length();i++)
		{
			flag=1;
			if(i+1<s1.length()&&(s1.charAt(i)==' '&&(s1.charAt(i+1)=='w'||s1.charAt(i+1)=='W')))
			{
				for(j=i+2,k=0;k<5&&j<s1.length();j++,k++)
				{
					if(s1.charAt(j)!=cccc1[k]&&s1.charAt(j)!=cccc1[k]+'A'-'a')
					{
						flag=0;
						break;
					}
				}
				
				if(flag==1&&j<s1.length())
				{
					return i;
				}
			}
		}
		return -1;
	}
	
	public QueryResult Execute(String statement)
	{
		int flag;
		int flag2;
		int flag3;
		int i,j,k,l;
		int position;
		int where_position;
		String s4=new String(statement);
		List<Instruction> Ins=null;
		
		position=index(statement);
		
		if(position==-1)
		{
			try{
				statements.add(Parser.parse(statement));
			}
			catch(Exception e)
			{
				System.out.println("Syntax error");
				return null;
			}
			
			Ins = CodeGenerator.GenerateByteCode(statements);
			
			//第一个and或者第一个or不会出现，修改让它出现
			
			if(where2(s4))
			{
				for(i=0;i<Ins.size();i++)
				{
					if(Ins.get(i).opCode==OpCode.BeginFilter)
					{
						break;
					}
				}
				//i+1i+1i+1加入
				for(j=i+1;j<Ins.size();)
				{
					if(Ins.get(j).opCode!=OpCode.EndFilter)
					{
						Ins.remove(j);
					}
					else {
						break;
					}
				}
				
				where_position=where3(s4);
				
				Integer c1_top=0;
				char [] c1=new char[40];
				
				flag3=0;//0开始是第一个列名
						//1是一个比较符号
						//2是一个具体列值
				for(j=where_position+7;j<statement.length();)
				{
					flag2=0;//0开始可能有空格，或可能有，号，或可能有单引号‘并且在最左边
						   //1在一段连续字符串的中间
						   //2在一段比较符号中间
					for(k=j;k<statement.length();)
					{
						if(statement.charAt(k)==')'||statement.charAt(k)=='('||statement.charAt(k)==' '||statement.charAt(k)==','||statement.charAt(k)=='\''||statement.charAt(k)=='\n'||statement.charAt(k)=='\t'||statement.charAt(k)=='"')
						{	
							if(flag2==0||flag2==2)
							{
								k++;
							}
							else if(flag2==1)
							{
								break;
							}
						}
						else if((statement.charAt(k)>='a'&&statement.charAt(k)<='z')||(statement.charAt(k)>='A'&&statement.charAt(k)<='Z')||(statement.charAt(k)>='0'&&statement.charAt(k)<='9')||statement.charAt(k)=='_')
						{
							if(flag2==2)
							{
								break;
							}
							else {
								flag2=1;
								c1[c1_top]=statement.charAt(k);
								c1_top++;
								k++;
							}
						}
						else if(statement.charAt(k)=='>'||statement.charAt(k)=='<'||statement.charAt(k)=='=')
						{
							if(flag2==1)
							{
								break;
							}
							else if(flag2==2)
							{
								c1[c1_top]=statement.charAt(k);
								c1_top++;
								k++;//比较符号的第二个符号不用留到下一次识别
								break;
							}
							else {
								flag2=2;
								c1[c1_top]=statement.charAt(k);
								c1_top++;
								k++;
							}
						}
						
						else {//处理汉字的情况
							if(flag2==2)
							{
								break;
							}
							else {
								flag2=1;
								c1[c1_top]=statement.charAt(k);
								c1_top++;
								k++;
							}
						}
					}
					
					j=k;
					
					String s2=new String(c1,0,c1_top);
					c1_top=0;
					
					if(s2.length()==0)
					{
						break;
					}
					
					else if(flag3==0)
					{
						flag3=1;
						OpCode o1=OpCode.Operand;
						Ins.add(i+1,new Instruction(o1,s2,null,null));
						i++;
					}
					
					else if(flag3==1)
					{
						flag3=2;
						OpCode o1=OpCode.Operator;
						switch(s2)
						{
							case ">":
							{
								s2="GT";
								break;
							}
							
							case ">=":
							{
								s2="GE";
								break;
							}
							
							case "<":
							{
								s2="LT";
								break;
							}
							
							case "<=":
							{
								s2="LE";
								break;
							}
							
							case "=":
							{
								s2="EQ";
								break;
							}
						}
						Ins.add(i+1,new Instruction(o1,s2,null,null));
						i++;
					}
					
					else if(flag3==2)
					{
						flag3=3;
						OpCode o1=OpCode.Operand;
						Ins.add(i,new Instruction(o1,null,s2,null));
						i++;
					}
					
					else if(flag3==3)
					{
						flag3=0;
						OpCode o1=OpCode.Operator;
						if(s2.equalsIgnoreCase("and"))
						{
							s2="And";
						}
						else if(s2.equalsIgnoreCase("or"))
						{
							s2="Or";
						}
						Ins.add(i+1,new Instruction(o1,s2,null,null));
						i++;
					}
				}
				
				String s3="Or";
				OpCode o1=OpCode.Operator;
				Ins.add(i+1,new Instruction(o1,s3,null,null));
			}
			
			
			/*flag=0;
			j=0;
			while(true)
			{
				i=this.where(statement);
				if(i!=-1)
				{
					j++;
					flag=1;
					statement=statement.substring(0,i)+statement.substring(i+4,statement.length());
					for(i=0;i<Ins.size();i++)
					{
						if(Ins.get(i).opCode.toString()=="BeginFilter")
						{
							break;
						}
					}
					OpCode o1=OpCode.Operator;
					
					if(j==1)
					{
						if(and_or==0)
						{
							Ins.add(i+4*j,new Instruction(o1,"And",null,null));
						}
						else {
							Ins.add(i+4*j,new Instruction(o1,"Or",null,null));
						}
					}
					
					else {
						if(and_or==0)
						{
							Ins.set(i+4*j,new Instruction(o1,"And",null,null));
						}
						else {
							Ins.set(i+4*j,new Instruction(o1,"Or",null,null));
						}
					}
				}
				
				else {
					if(flag==1)
					{
						for(i=0;i<Ins.size();i++)
						{
							if(Ins.get(i).opCode.toString()=="BeginFilter")
							{
								break;
							}
						}
						while(Ins.get(i+4*(j+1)).opCode==OpCode.Operator)
						{
							Ins.remove(i+4*(j+1));
							i++;
						}
						Ins.add(i+4*(j+1)-1,new Instruction(OpCode.Operator,"Or",null,null));//多加一个or
					}
					
					else if(where2(s4))
					{
						for(i=0;i<Ins.size();i++)
						{
							if(Ins.get(i).opCode.toString()=="BeginFilter")
							{
								break;
							}
						}
						Ins.add(i+4,new Instruction(OpCode.Operator,"Or",null,null));//多加一个or
					}
					
					break;
				}
			}*/
		}
		else {
			Ins=new ArrayList<Instruction>();
			Ins.add(new Instruction(OpCode.Transaction,null,null,null));
			for(i=position+7;i<statement.length();i++)
			{
				if(statement.charAt(i)==' ')
				{
					break;
				}
			}
			String s1=statement.substring(position+7, i);
			
			j=i+4;
			for(i=i+4;i<statement.length();i++)
			{
				if(statement.charAt(i)==' '||statement.charAt(i)=='(')
				{
					break;
				}
			}
			String s2=statement.substring(j, i);
			
			Ins.add(new Instruction(OpCode.Index, s1 , s2, null));//p1存索引名字，p2存索引所在的表名
			
			for(;i<statement.length();i++)
			{
				if(statement.charAt(i)=='(')
				{
					break;
				}
			}
			
			for(i++;i<statement.length();i++)
			{
				if(statement.charAt(i)!=' '&&statement.charAt(i)!='('&&statement.charAt(i)!=','&&statement.charAt(i)!=')')
				{
					for(j=i;j<statement.length();j++)
					{
						if(statement.charAt(j)==' '||statement.charAt(j)=='('||statement.charAt(j)==','||statement.charAt(j)==')')
						{
							break;
						}
					}
					String s3=statement.substring(i, j);
					Ins.add(new Instruction(OpCode.Index_Column, s3, null, null));
					i=j;
				}
			}
			
			Ins.add(new Instruction(OpCode.Execute, null, null, null));
			
			Ins.add(new Instruction(OpCode.Commit, null, null, null));
			
		}
		
		Ins.add(1,new Instruction(OpCode.Begin, null, null, null));
		
		statements.clear();
		//for(int i=0;i<Ins.size();i++)
		//    System.out.println(Ins.get(i));
		
		try {
			return vm.run(Ins);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public void dump() throws IOException, ClassNotFoundException
	{
		File db_file = new File(db.dBName);
		FileChannel fc = null;
		RandomAccessFile fin = new RandomAccessFile(db_file, "rw");
		fc = fin.getChannel();
		ByteBuffer fileHeader = ByteBuffer.allocate(db.cacheManager.FILEHEADERSIZE);
		fc.read(fileHeader, 0);
		
		
		byte [] bytes=new byte[Page.PAGE_SIZE];
	    fileHeader.rewind();
	    fileHeader.get(bytes,0,fileHeader.remaining());
	       
	    ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
        ObjectInputStream objTable=new ObjectInputStream(byteTable);
        
        Integer ii1;
        Integer ii2;
        ii1=(Integer)objTable.readObject();
	    ii2=(Integer)objTable.readObject();
	    
	    String s1=db.dBName+"_dump.txt";
	    FileWriter fw1=new FileWriter(s1);
	    BufferedWriter bw1=new BufferedWriter(fw1);
	    String s2="对原来数据库文件没有完全按照定义的格式dump原文件的一些内容也没有dump\n\n";
	    
	    bw1.write(s2);
	    
	    bw1.write("文件已分配的最大页号："+ii1+"\n");
	    bw1.write("文件master表B树根节点所在页号："+ii2+"\n");
	    
	    fileHeader.clear();
	    fileHeader.rewind();
	    fc.read(fileHeader, db.cacheManager.FILEHEADERSIZE);
	    
	    fileHeader.rewind();
	    fileHeader.get(bytes,0,fileHeader.remaining());
	    
	    byteTable=new ByteArrayInputStream(bytes);
        objTable=new ObjectInputStream(byteTable);
	    
	    Integer i,i1;
	    int flag1;
	    int flag2=0;
	    
	    i1=(Integer)objTable.readObject();
	    
	    if(i1!=-1)
	    {
		    while(true)
	        {
	        	flag1=0;
	        	for(i=0;i<50;i++)
	        	{
	        		i1=(Integer)objTable.readObject();
	        		if(i1!=-1)
	        		{
	        			if(flag2==0)
	        			{
	        				bw1.write("空闲页为："+i1+"\n");
	        				flag2=1;
	        			}
	        			else {
	        				bw1.write(i1+"\n");
	        			}
	        		}
	        		else {
	        			if(flag2==0)
	        			{
	        				bw1.write("没有空闲页\n");
	        			}
	        			flag1=1;
	        			break;
	        		}
	        	}
	        	
	        	if(flag1==1)
	        	{
	        		bw1.write("\n");
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
	    
	    else {
	    	bw1.write("没有空闲页\n\n");
	    }
	    
	    npu.zunsql.treemng.Transaction tran=db.beginWriteTrans();
	    Node n0=new Node(-2,db.cacheManager,tran);
	    
	    
	    int j,k;
	    Node n1,n2,p1,p2;
	    Stack<Node>st1=new Stack<Node>();
	    Stack<Node_itree>st2=new Stack<Node_itree>();
	    Node_index ni1;
	    Stack<Node>master=new Stack<Node>();
	    
	    master.push(n0);
    	while(!master.empty())
    	{
    		n1=master.pop();
    		if(n1.sonNodeList.size()!=0)
    		{
    			n2=new Node(n1.sonNodeList.get(0),db.cacheManager,tran);
    			master.push(n2);
	    		for(k=0;k<n1.rowList.size();k++)
	    		{
	    			st1.push(new Node(Integer.parseInt(n1.rowList.get(k).cellList.get(1).getValue_s()),db.cacheManager,tran));
	    	    	flag1=0;
	    	    	while(!st1.empty())
	    	    	{
	    	    		p1=st1.pop();
	    	    		if(p1.sonNodeList.size()!=0)
	    	    		{
	    	    			if(flag1==0)
	    	    			{
	    	    				flag1=1;
	    	    				bw1.write(p1.node_tablename+"表"+"B树根节点页号："+p1.pageOne+"\n");
	    	    				bw1.write(p1.node_tablename+"表"+"主键："+p1.keyName+"\n");
	    	    				for(i=1;i<=p1.columnname.size();i++)
	    	    				{
	    	    					bw1.write("第"+i+"列列名："+p1.columnname.get(i-1)+"\n");
	    	    					bw1.write("第"+i+"列列类型："+p1.columntype.get(i-1)+"\n");
	    	    				}
	    	    				bw1.write("\n");
	    	    			}
	    	    			
	    	    			p2=new Node(p1.sonNodeList.get(0),db.cacheManager,tran);
	    	    			st1.push(p2);
	    	    			
	    	    			bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页儿子节点页号：\n");
	    	    			for(j=0;j<p1.sonNodeList.size();j++)
	    	    			{
	    	    				bw1.write(p1.sonNodeList.get(j)+" ");
	    	    			}
	    	    			bw1.write("\n");
	    	    			
	    	    			if(p1.rowList.size()!=0)
	    	    			{
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页记录：\n");
		    		    		for(i=0;i<p1.rowList.size();i++)
		    		    		{
		    		    			for(j=0;j<p1.rowList.get(i).cellList.size();j++)
		    		    			{
		    		    				bw1.write(p1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    			}
		    		    			bw1.write("\n");
		    		    			p2=new Node(p1.sonNodeList.get(i+1),db.cacheManager,tran);
		    		    			st1.push(p2);
		    		    		}
	    	    			}
	    	    			
	    	    			else {
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页没有记录\n");
	    	    			}
	    		    		
	    		    		bw1.write("\n");
	    		    		
	    	    		}
	    	    		else {
	    	    			if(flag1==0)
	    	    			{
	    	    				flag1=1;
	    	    				bw1.write(p1.node_tablename+"表"+"B树根节点页号："+p1.pageOne+"\n");
	    	    				bw1.write(p1.node_tablename+"表"+"主键："+p1.keyName+"\n");
	    	    				for(i=1;i<=p1.columnname.size();i++)
	    	    				{
	    	    					bw1.write("第"+i+"列列名："+p1.columnname.get(i-1)+"\n");
	    	    					bw1.write("第"+i+"列列类型："+p1.columntype.get(i-1)+"\n");
	    	    				}
	    	    				bw1.write("\n");
	    	    			}
	    	    			
	    	    			bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页没有儿子页");
	    	    			bw1.write("\n");
	    	    			
	    	    			if(p1.rowList.size()!=0)
	    	    			{
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页记录：\n");
		    	    			for(i=0;i<p1.rowList.size();i++)
		    		    		{
		    	    				for(j=0;j<p1.rowList.get(i).cellList.size();j++)
		    		    			{
		    		    				bw1.write(p1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    			}
		    		    			bw1.write("\n");
		    		    		}
	    	    			}
	    	    			
	    	    			else {
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页没有记录\n");
	    	    			}

	    	    			bw1.write("\n");
	    	    		}
	    	    	}
	    	    	
	    	    	bw1.flush();
	    	    	st1.clear();
	    	    	
	    	    	if(Integer.parseInt(n1.rowList.get(k).getCell(2).getValue_s())!=-1)
	    	    	{
	    	    		ni1=new Node_index(Integer.parseInt(n1.rowList.get(k).getCell(2).getValue_s()),db.cacheManager,tran);
	    	    		int x;
	    	    		Node_itree t1,t2;
	    	    		while(true)
	    	        	{
	    	    			for(x=0;x<ni1.indexpages.size();x++)
	    	    			{
	    	    				st2.push(new Node_itree(ni1.indexpages.get(x),db.cacheManager,tran));	    	    				
	    		    	    	flag1=0;
	    		    	    	while(!st2.empty())
	    		    	    	{
	    		    	    		t1=st2.pop();
	    		    	    		if(t1.sonNodeList.size()!=0)
	    		    	    		{
	    		    	    			if(flag1==0)
	    		    	    			{
	    		    	    				flag1=1;
	    		    	    				bw1.write(t1.tablename+"的索引表"+"B树根节点页号："+t1.pageOne+"\n");
	    		    	    				bw1.write(t1.tablename+"的索引表"+"索引名："+t1.indexname+"\n");
	    		    	    				for(i=1;i<=t1.keyname.size();i++)
	    		    	    				{
	    		    	    					bw1.write("第"+i+"个建立索引的列名："+t1.keyname.get(i-1)+"\n");
	    		    	    				}
	    		    	    				bw1.write("\n");
	    		    	    			}
	    		    	    			
	    		    	    			t2=new Node_itree(t1.sonNodeList.get(0),db.cacheManager,tran);
	    		    	    			st2.push(t2);
	    		    	    			
	    		    	    			bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页儿子节点页号：\n");
	    		    	    			for(j=0;j<t1.sonNodeList.size();j++)
	    		    	    			{
	    		    	    				bw1.write(t1.sonNodeList.get(j)+" ");
	    		    	    			}
	    		    	    			bw1.write("\n");
	    		    	    			
	    		    	    			if(t1.rowList.size()!=0)
	    		    	    			{
	    		    	    				bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页记录：\n");
		    		    		    		for(i=0;i<t1.rowList.size();i++)
		    		    		    		{
		    		    		    			for(j=0;j<t1.rowList.get(i).cellList.size();j++)
		    		    		    			{
		    		    		    				bw1.write(t1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    		    			}
		    		    		    			bw1.write("\n");
		    		    		    			t2=new Node_itree(t1.sonNodeList.get(i+1),db.cacheManager,tran);
		    		    		    			st2.push(t2);
		    		    		    		}
	    		    	    			}
	    		    	    			
	    		    	    			else {
	    		    	    				bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页没有记录\n");
	    		    	    			}
	    		    		    		bw1.write("\n");
	    		    		    		
	    		    	    		}
	    		    	    		else {
	    		    	    			if(flag1==0)
	    		    	    			{
	    		    	    				flag1=1;
	    		    	    				bw1.write(t1.tablename+"的索引表"+"B树根节点页号："+t1.pageOne+"\n");
	    		    	    				bw1.write(t1.tablename+"的索引表"+"索引名："+t1.indexname+"\n");
	    		    	    				for(i=1;i<=t1.keyname.size();i++)
	    		    	    				{
	    		    	    					bw1.write("第"+i+"个建立索引的列名："+t1.keyname.get(i-1)+"\n");
	    		    	    				}
	    		    	    				bw1.write("\n");
	    		    	    			}
	    		    	    			
	    		    	    			bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页没有儿子页");
	    		    	    			bw1.write("\n");
	    		    	    			
	    		    	    			if(t1.rowList.size()!=0)
	    		    	    			{
	    		    	    				bw1.write(t1.tablename+"索引表第"+t1.pageOne+"页记录：\n");
		    		    	    			for(i=0;i<t1.rowList.size();i++)
		    		    		    		{
		    		    	    				for(j=0;j<t1.rowList.get(i).cellList.size();j++)
		    		    		    			{
		    		    		    				bw1.write(t1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    		    			}
		    		    		    			bw1.write("\n");
		    		    		    		}
	    		    	    			}
	    		    	    			
	    		    	    			else {
	    		    	    				bw1.write(t1.tablename+"索引表第"+t1.pageOne+"页没有记录\n");
	    		    	    			}
	    		    	    			bw1.write("\n");
	    		    	    		}
	    		    	    	}
	    		    	    	
	    		    	    	bw1.flush();
	    		    	    	st1.clear();
	    	    			}
    	    	    		if(ni1.sonpage!=-1)
    	    	    		{
    	    	    			ni1=new Node_index(ni1.sonpage,db.cacheManager,tran);
    	    	    			continue;
    	    	    		}
    	    	    		else {
    	    	    			break;
    	    	    		}
	    	        	}
	    	    		
	    	    	}
	    			
	    			n2=new Node(n1.sonNodeList.get(k+1),db.cacheManager,tran);
	    			master.push(n2);
	    		}
    		}
    		else {
    			for(k=0;k<n1.rowList.size();k++)
	    		{
	    			st1.push(new Node(Integer.parseInt(n1.rowList.get(k).cellList.get(1).getValue_s()),db.cacheManager,tran));
	    	    	flag1=0;
	    	    	while(!st1.empty())
	    	    	{
	    	    		p1=st1.pop();
	    	    		if(p1.sonNodeList.size()!=0)
	    	    		{
	    	    			if(flag1==0)
	    	    			{
	    	    				flag1=1;
	    	    				bw1.write(p1.node_tablename+"表"+"B树根节点页号："+p1.pageOne+"\n");
	    	    				bw1.write(p1.node_tablename+"表"+"主键："+p1.keyName+"\n");
	    	    				for(i=1;i<=p1.columnname.size();i++)
	    	    				{
	    	    					bw1.write("第"+i+"列列名："+p1.columnname.get(i-1)+"\n");
	    	    					bw1.write("第"+i+"列列类型："+p1.columntype.get(i-1)+"\n");
	    	    				}
	    	    				bw1.write("\n");
	    	    			}
	    	    			
	    	    			p2=new Node(p1.sonNodeList.get(0),db.cacheManager,tran);
	    	    			st1.push(p2);
	    	    			
	    	    			bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页儿子节点页号：\n");
	    	    			for(j=0;j<p1.sonNodeList.size();j++)
	    	    			{
	    	    				bw1.write(p1.sonNodeList.get(j)+" ");
	    	    			}
	    	    			bw1.write("\n");
	    	    			
	    	    			if(p1.rowList.size()!=0)
	    	    			{
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页记录：\n");
		    		    		for(i=0;i<p1.rowList.size();i++)
		    		    		{
		    		    			for(j=0;j<p1.rowList.get(i).cellList.size();j++)
		    		    			{
		    		    				bw1.write(p1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    			}
		    		    			bw1.write("\n");
		    		    			p2=new Node(p1.sonNodeList.get(i+1),db.cacheManager,tran);
		    		    			st1.push(p2);
		    		    		}
	    	    			}
	    	    			
	    	    			else {
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页没有记录\n");
	    	    			}
	    		    		bw1.write("\n");
	    		    		
	    	    		}
	    	    		else {
	    	    			if(flag1==0)
	    	    			{
	    	    				flag1=1;
	    	    				bw1.write(p1.node_tablename+"表"+"B树根节点页号："+p1.pageOne+"\n");
	    	    				bw1.write(p1.node_tablename+"表"+"主键："+p1.keyName+"\n");
	    	    				for(i=1;i<=p1.columnname.size();i++)
	    	    				{
	    	    					bw1.write("第"+i+"列列名："+p1.columnname.get(i-1)+"\n");
	    	    					bw1.write("第"+i+"列列类型："+p1.columntype.get(i-1)+"\n");
	    	    				}
	    	    				bw1.write("\n");
	    	    			}
	    	    			
	    	    			bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页没有儿子页");
	    	    			bw1.write("\n");
	    	    			
	    	    			if(p1.rowList.size()!=0)
	    	    			{
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页记录：\n");
		    	    			for(i=0;i<p1.rowList.size();i++)
		    		    		{
		    	    				for(j=0;j<p1.rowList.get(i).cellList.size();j++)
		    		    			{
		    		    				bw1.write(p1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    			}
		    		    			bw1.write("\n");
		    		    		}
	    	    			}
	    	    			else {
	    	    				bw1.write(p1.node_tablename+"表第"+p1.pageOne+"页没有记录\n");
	    	    			}
	    	    			bw1.write("\n");
	    	    		}
	    	    	}
	    	    	
	    	    	bw1.flush();
	    	    	st1.clear();
	    	    	
	    	    	if(Integer.parseInt(n1.rowList.get(k).getCell(2).getValue_s())!=-1)
	    	    	{
	    	    		ni1=new Node_index(Integer.parseInt(n1.rowList.get(k).getCell(2).getValue_s()),db.cacheManager,tran);
	    	    		int x;
	    	    		Node_itree t1,t2;
	    	    		while(true)
	    	        	{
	    	    			for(x=0;x<ni1.indexpages.size();x++)
	    	    			{
	    	    				st2.push(new Node_itree(ni1.indexpages.get(x),db.cacheManager,tran));	    	    				
	    		    	    	flag1=0;
	    		    	    	while(!st2.empty())
	    		    	    	{
	    		    	    		t1=st2.pop();
	    		    	    		if(t1.sonNodeList.size()!=0)
	    		    	    		{
	    		    	    			if(flag1==0)
	    		    	    			{
	    		    	    				flag1=1;
	    		    	    				bw1.write(t1.tablename+"的索引表"+"B树根节点页号："+t1.pageOne+"\n");
	    		    	    				bw1.write(t1.tablename+"的索引表"+"索引名："+t1.indexname+"\n");
	    		    	    				for(i=1;i<=t1.keyname.size();i++)
	    		    	    				{
	    		    	    					bw1.write("第"+i+"个建立索引的列名："+t1.keyname.get(i-1)+"\n");
	    		    	    				}
	    		    	    				bw1.write("\n");
	    		    	    			}
	    		    	    			
	    		    	    			t2=new Node_itree(t1.sonNodeList.get(0),db.cacheManager,tran);
	    		    	    			st2.push(t2);
	    		    	    			
	    		    	    			bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页儿子节点页号：\n");
	    		    	    			for(j=0;j<t1.sonNodeList.size();j++)
	    		    	    			{
	    		    	    				bw1.write(t1.sonNodeList.get(j)+" ");
	    		    	    			}
	    		    	    			bw1.write("\n");
	    		    	    			
	    		    	    			if(t1.rowList.size()!=0)
	    		    	    			{
	    		    	    				bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页记录：\n");
		    		    		    		for(i=0;i<t1.rowList.size();i++)
		    		    		    		{
		    		    		    			for(j=0;j<t1.rowList.get(i).cellList.size();j++)
		    		    		    			{
		    		    		    				bw1.write(t1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    		    			}
		    		    		    			bw1.write("\n");
		    		    		    			t2=new Node_itree(t1.sonNodeList.get(i+1),db.cacheManager,tran);
		    		    		    			st2.push(t2);
		    		    		    		}
	    		    	    			}
	    		    	    			
	    		    	    			else {
	    		    	    				bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页没有记录\n");
	    		    	    			}
	    		    	    			
	    		    		    		bw1.write("\n");
	    		    		    		
	    		    	    		}
	    		    	    		else {
	    		    	    			if(flag1==0)
	    		    	    			{
	    		    	    				flag1=1;
	    		    	    				bw1.write(t1.tablename+"的索引表"+"B树根节点页号："+t1.pageOne+"\n");
	    		    	    				bw1.write(t1.tablename+"的索引表"+"索引名："+t1.indexname+"\n");
	    		    	    				for(i=1;i<=t1.keyname.size();i++)
	    		    	    				{
	    		    	    					bw1.write("第"+i+"个建立索引的列名："+t1.keyname.get(i-1)+"\n");
	    		    	    				}
	    		    	    				bw1.write("\n");
	    		    	    			}
	    		    	    			
	    		    	    			bw1.write(t1.tablename+"的索引表第"+t1.pageOne+"页没有儿子页");
	    		    	    			bw1.write("\n");
	    		    	    			
	    		    	    			if(t1.rowList.size()!=0)
	    		    	    			{
	    		    	    				bw1.write(t1.tablename+"索引表第"+t1.pageOne+"页记录：\n");
		    		    	    			for(i=0;i<t1.rowList.size();i++)
		    		    		    		{
		    		    	    				for(j=0;j<t1.rowList.get(i).cellList.size();j++)
		    		    		    			{
		    		    		    				bw1.write(t1.rowList.get(i).cellList.get(j).getValue_s()+" ");
		    		    		    			}
		    		    		    			bw1.write("\n");
		    		    		    		}
	    		    	    			}
	    		    	    			
	    		    	    			else {
	    		    	    				bw1.write(t1.tablename+"索引表第"+t1.pageOne+"页没有记录\n");
	    		    	    			}
	    		    	    			
	    		    	    			bw1.write("\n");
	    		    	    		}
	    		    	    	}
	    		    	    	
	    		    	    	bw1.flush();
	    		    	    	st1.clear();
	    	    			}
    	    	    		if(ni1.sonpage!=null&&ni1.sonpage!=-1)
    	    	    		{
    	    	    			ni1=new Node_index(ni1.sonpage,db.cacheManager,tran);
    	    	    			continue;
    	    	    		}
    	    	    		else {
    	    	    			break;
    	    	    		}
	    	        	}	    	    		
	    	    	}
	    		}
    		}
    	}
	    
	    
	    bw1.flush();
	    bw1.close();
		
	}
	
	public void Close()
	{
		try {
			db.close();
			statements.clear();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
