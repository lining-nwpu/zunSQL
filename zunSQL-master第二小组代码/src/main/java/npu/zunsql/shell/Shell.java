package npu.zunsql.shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import npu.zunsql.DBInstance;
import npu.zunsql.virenv.QueryResult;
import npu.zunsql.DBInstance;

public class Shell 
{
	public static void main(String[] args) throws IOException, ClassNotFoundException 
	{
		// TODO Auto-generated method stub
		//set multi-DBInstance
		DBInstance dbinstance[] = new DBInstance[10];
		String[] DB= new String[10];
	    String worked_DB = "";
		int p1;
		int flag1;
	    int DB_num = 0;
		byte []b1=new byte[300];
		String printwords = "zunSQL>";
		String commandlist = "";
		System.out.println("please type in . before any command(except sql command)");
		System.out.println();
		System.out.println("sql command must end with ;");
		System.out.println();
		System.out.println("示例1：create table student(stuno int primary key, ");
		System.out.println("sname varchar,score double,course varchar);");
		System.out.println();
		System.out.println("示例2：create index in1 on student(sname,course);");
		System.out.println();
		System.out.println("示例3：insert into student (stuno, sname, score, course) ");
		System.out.println("values (2017004, '了', 90, 'DS');");
		System.out.println();
		System.out.println("示例4：create table teacher(stuno2 int primary key, ");
		System.out.println("score2 double,course2 varchar);");
		System.out.println();
		System.out.println("示例5：select stuno, course from student where stuno>=2017004 and sname='la';");
		System.out.println();
		System.out.println("示例6：update student set score=666,stuno=98765 where sname='la';");
		System.out.println();
		System.out.println("示例7：select stuno course from student where stuno>=200 and sname='la';");
		System.out.println();
		System.out.println("示例8：select * from student,teacher where stuno>=200 and sname='la';");
		System.out.println();
		System.out.println("示例9：delete from student where stuno>=2017004 and sname='lke';");
		System.out.println();
		
		
		System.out.println("如果遇到问题请删除原文件后再次执行");
		System.out.println();
		
		System.out.println("Enter \".help\" for usage hints.  .open打开数据库,.close打开以后关闭数据库");
		System.out.println();
		
		System.out.println("推荐使用App.java直接执行和修改sql命令，不推荐使用该shell.java");
		System.out.println();
		
		System.out.println("please check sql command must end with ; sql命令请以分号结尾");
		System.out.println();
		
		System.out.println("please check sql command must end with ; sql命令请以分号结尾");
		System.out.println();
		
		while(true)
		{
		    p1=0;
		    
			System.out.print(printwords);
		    //input a CMD
		    //Scanner scan = new Scanner(System.in);
		    
		    while(true)
		    {
		    	
			    System.in.read(b1, p1, 200-p1);
			    
			    for(;p1<=200;p1++)
			    {
			    	if(b1[p1]==0)
			    	{
			    		break;
			    	}
			    }
			    
			    if(b1[p1-1]=='\n')
			    {
			    	b1[p1-1]=0;
			    	p1--;
			    }
			    
			    if(b1[p1-1]=='\r')
			    {
			    	b1[p1-1]=0;
			    	p1--;
			    }
			    
			    if(p1==0)
			    {		
			    	break;
			    }
			    
			    if(b1[p1-1]==';')
			    {
			    	b1[p1-1]=0;
			    	p1--;
			    	break;
			    }
			    
			    if(b1[0]=='.')
			    {
			    	break;
			    }
			    
		    }
		    
		    if(p1==0)
		    {
		    	continue;
		    }
		    
		    String user_command="";
		    
		    String user_command2=new String(b1,0,p1);
		    
		    for(int j=0;j<user_command2.length();j++)
		    {
		    	if(user_command2.charAt(j)!='\r'&&user_command2.charAt(j)!='\n')
		    	{
		    		user_command=user_command+user_command2.charAt(j);
		    	}
		    }
		    
		    //String user_command = scan.nextLine();
		    //check enter
		    if(user_command.length() == 0)
		    {
		    	continue;
		    }
		    //analyse the user_command
		    switch(GetKeyword(user_command))
		    {
		    	case ".dump":
		    		dbinstance[CheckDBName(worked_DB, DB, DB_num)].dump();
		    		break;
		        //.help
		        case ".help":
			   	    HelpInfor();
		            break;
		        //.open
		        case ".open":
		        	//find the DBName
		    	    String DBName = MakeCMD(user_command);
		    	    if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
		    	    if(CheckDBName(DBName, DB, DB_num) == -1)
		    	    {
		    	    	//DB is full
		    	    	if(DB_num == 10)
		    	    	{
		    	    		System.out.println("DB is full.");
		    	    		break;
		    	    	}
		    	    	DB[GetDBNo(DBName, DB)] = DBName;
			    	    DB_num++;
			    	    //open db
			    	    dbinstance[CheckDBName(DBName, DB, DB_num)] = DBInstance.Open(DBName);
		    	    }
		    	    worked_DB = DBName;
		    	    printwords = '[' + worked_DB + "]>";
		            break;
				//.close
		        case ".close":
		        	dbinstance[CheckDBName(worked_DB, DB, DB_num)].Close();
		        	printwords = "zunSQL>";
	        		worked_DB = "";
		        	/*DBName = MakeCMD(user_command);
		        	if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
		        	int flag = CheckDBName(DBName, DB, DB_num);
		        	if(flag == -1)
		    	    {
		        		System.out.println(DBName + " is not opened.");
		    	    }
		        	else
		        	{
		        		dbinstance[flag].Close();
		        		DB[flag] = null;
		        		dbinstance[flag] = null;
			        	if(DBName.equals(worked_DB))
			        	{
			        		for(int i = 0;i < 10;i++)
			        		{
		        				worked_DB = DB[i];
			        			if(DB[i] != null)
			        			{
	                                break;
			        			}
			        		}
			        		if(worked_DB == null) 
			        		{
			        			printwords = "zunSQL>";
				        		worked_DB = "";
			        		}
			        		else
			        		{
				        		printwords = '[' + worked_DB + "]>";
			        		}
			        	}
		        	}*/
				    break;
		        default:
			    //unmatched 
		        {
		        	if(user_command.charAt(0) == '.')
		        	{
		        		System.out.println(GetKeyword(user_command) + " is not defined.");
		        	}
		        	//SQL
		        	else
		        	{
		        		//check worked_DB
		        		if(worked_DB == "")
		        		{
		        			System.out.println("please open a database firstly.");
		        			break;
		        		}		        		
		        		//execute the SQL
		        		else {
		        			commandlist = commandlist + user_command;
		        			
		        			DoSQL(dbinstance[CheckDBName(worked_DB, DB, DB_num)],commandlist);
		        			
		        			commandlist = "";
		        			
		        			printwords = '[' + worked_DB + "]>";
		        		}
		        		/*else
		        		{
		        			commandlist = commandlist + user_command;
		        			printwords = "您的输入有问题....>";
		        			break;
		        		}*/
		            }
		            break;
		        }
		    }
		    
		    for(;p1>=0;p1--)
		    {
		    	b1[p1]=0;
		    }
		    
		}
	}
	
	public static void DoSQL(DBInstance dbInstance, String user_command) 
	{
		// TODO Auto-generated method stub
		QueryResult result;
		result = dbInstance.Execute(user_command);   

	}
	public static int GetDBNo(String DBName, String[] DB)
	{
		for(int i = 0;i < 10;i++)
		{
			if(DB[i] == null)
			{
			    return i;	
			}
		}
		return -1;
	}
	//CheckDBName
	public static int CheckDBName(String DBName, String[] DB, int DB_num) 
	{
		for(int i = 0;i < 10;i++)
		{
			if(DB[i]==null)
			{
				continue;
			}
			if(DB[i].equals(DBName))
			{
			    return i;	
			}
		}
		//if DBName is not in the DB return -1
		return -1;
	}
	//find the keywords in user_command
	public static String GetKeyword(String user_command)
	{
		if(user_command == "")
		{
			return "";
		}
		if(user_command.charAt(0) == '.')
		{
			String keywords = ""; 
			for(int i = 0;i < user_command.length();i++)
			{
				if(user_command.charAt(i) == ' ')
				{
					break;
				}
				keywords = keywords + user_command.charAt(i);
			}
			return keywords;
		}
		else
		{
			return user_command;
		}
	}
	public static String MakeCMD(String user_command) 
	{
	    String DBName = ""; 
	    int flag = 0;
	    int i;
		for(i = 0;i < user_command.length();i++)
		{
			if(flag == 1)
			{
				DBName = DBName + user_command.charAt(i);
			}
			if(user_command.charAt(i) == ' ')
			{
				flag++;
			}
		}
		if(flag != 1)
		{
			if(flag == 0)
			{
				DBName = " #The string name is empty.";
			}
			else
			{
				for(int j = user_command.length() - 1;j > user_command.length() - flag;j--)
				{
					if(user_command.charAt(j) !=' ')
					{
						DBName = " #DBName can not have ' '.";
						break;
					}
				}
				if(DBName.charAt(0) != ' ')
				{
					DBName = DBName.substring(0, DBName.length() - 1);
				}
			}
		}
		else
		{
			if(user_command.charAt(user_command.length() - 3) != '.'
					|| user_command.charAt(user_command.length() - 2) != 'd'
					|| user_command.charAt(user_command.length() - 1) != 'b')
			{
				DBName = " #\"" + DBName + "\" is incorrect.";
			}
		}
		return DBName; 
	}
	//information
	public static void HelpInfor() 
	{
		System.out.println(".open  *.db    Open a database or create a new database");
		System.out.println(".close *.db    Close a database");
		System.out.println(".help          Show this message");
		System.out.println(".dump          把java序列化的二进制文件以实际表格的形式输出到另一个文件，需要用.open已经打开一个数据库文件");
	}
}
