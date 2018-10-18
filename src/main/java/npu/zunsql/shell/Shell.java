package npu.zunsql.shell;

import java.util.Scanner;
import npu.zunsql.DBInstance;
import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;
import npu.zunsql.cache.Transaction;
import npu.zunsql.virenv.QueryResult;
import sun.misc.Cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import npu.zunsql.DBInstance;

public class Shell 
{
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		//�������ݿ�Ŀ���
		DBInstance dbinstance[] = new DBInstance[10];
		String[] DB= new String[10];
	    String worked_DB = "";
		int DB_num = 0;
		QueryResult result;
		String printwords = "zunSQL>";
		while(true)
		{
		    System.out.print(printwords);
		    //��ȡһ���û�����
		    Scanner scan = new Scanner(System.in);
		    String user_command = scan.nextLine();
		    //��������س��ж�
		    if(user_command.length() == 0)
		    {
		    	continue;
		    }
		    //�����û�����
		    switch(GetKeyword(user_command))
		    {
		        //.help
		        case ".help":
			   	    HelpInfor();
		            break;
		        //.open
		        case ".open":		        
		    	    String DBName = MakeCMD(user_command);
		    	    if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
		    	    if(CheckDBName(DBName, DB, DB_num) == -1)
		    	    {
		    	    	DB[DB_num] = DBName;
			    	    DB_num++;
			    	    //�����ݿ�
			    	    dbinstance[CheckDBName(DBName, DB, DB_num)] = DBInstance.Open(DBName);
		    	    }
		    	    worked_DB = DBName;
		    	    printwords = '[' + worked_DB + "]>";
		            break;
				//.close
		        case ".close":
		        	DBName = MakeCMD(user_command);
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
		        		DB[flag] = "";
			        	if(DBName.equals(worked_DB))
			        	{
			        		printwords = "zunSQL>";
			        		worked_DB = "";
			        	}
		        	}
				    break;
				//.work
		        case ".work":
			        DBName = MakeCMD(user_command);
			        if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
			        if(CheckDBName(DBName, DB, DB_num)==-1)
		    	    {
			        	System.out.println(DBName + " is not opened.");
		    	    }
			        else
			        {
			        	worked_DB = DBName;
			        	printwords = '[' + worked_DB + "]>";
			        }
					break;
		        default:
			    //ƥ��ʧ��
		        {
		        	if(user_command.charAt(0) == '.')
		        	{
		        		System.out.println(GetKeyword(user_command) + " is not defined.");
		        	}
		        	//ִ��SQL���
		        	else
		        	{
		        		if(worked_DB == "")
		        		{
		        			System.out.println("please open a database firstly.");
		        			break;
		        		}
		        		//���ݷ���result�Ľ��
		                result = dbinstance[CheckDBName(worked_DB, DB, DB_num)].Execute(user_command);   
		                if(result != null)
		                {
		                	if(result.getHeader() != null)
		                	{
			                	System.out.println(result.getRes());
		                	}
		                }
		            }
		            break;
		        }
		    }
		    
		}
	}
	public static int CheckDBName(String DBName, String[] DB, int DB_num) 
	{
		for(int i = 0;i < DB_num;i++)
		{
			if(DB[i].equals(DBName))
			{
			    return i;	
			}
		}
		return -1;
	}
	//ɸѡ����Ĺؼ��ֶ�
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
	//�����ݿ����ƣ�û�н��������ж�
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
			if(flag == 0)//ʲô��û�ж����������
			{
				DBName = " #The string name is empty.";
				//���صĴ�����Ϣǰ������ո�ȷ������Ͷ���������ͻ
			}
			else//�������ֿո�
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
			//���ݿ����Ƹ�ʽ���ԣ������λ����.db
			{
				DBName = " #\"" + DBName + "\" is incorrect.";
			}
		}
		return DBName; 
	}
	//����
	public static void HelpInfor() 
	{
		System.out.println(".open  *.db    Open a database");
		System.out.println(".close *.db    Close a database");
		System.out.println(".work *.db    Update the * database to the current database");
		System.out.println(".help          Show this message");
	}
}
