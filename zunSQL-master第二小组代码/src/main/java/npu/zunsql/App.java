package npu.zunsql;

import npu.zunsql.DBInstance;
import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;
import npu.zunsql.cache.Transaction;
import npu.zunsql.virenv.QueryResult;
//import sun.misc.Cache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Hello world！需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
 */
public class App {
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		/*
		 * test for readPage and writePage in cacheMgr, when a transation commit
		 * CacheMgr cacheManager = new CacheMgr("student"); int tranID =
		 * cacheManager.beginTransation("w");
		 * 
		 * ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
		 * tempBuffer.putInt(0,987); Page tempPage = new Page(tempBuffer);
		 * cacheManager.writePage(tranID, tempPage); try {
		 * cacheManager.commitTransation(tranID); } catch (IOException e) {
		 * e.printStackTrace(); } Page rPage = cacheManager.readPage(2,
		 * tempPage.getPageID()); int ret = rPage.getPageBuffer().getInt(0);
		 * System.out.println(ret);
		 * 
		 */

		//////////////////////////////////////////////////////
		//支持多次打开同一个文件，如果test.db已经存在，则会保留原有的表，而在之后再加
		//支持多次打开同一个文件
		//支持多次打开同一个文件
		//支持多次打开同一个文件
		//支持多次打开同一个文件
		//支持多次打开同一个文件
		//支持多次打开同一个文件
		//支持多次打开同一个文件
		//////////////////////////////////////////////////////
		///////////////////////////////////////////////////////
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误，如果中途打断需要删除原文件
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		/////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////
		DBInstance dbinstance = DBInstance.Open("test.db", 5);
		QueryResult result;
		//dump
		dbinstance.dump();
		
		//创建超过5张表
		//创建超过5张表
		result = dbinstance.Execute("create table student(stuno int, sname varchar,score double,course varchar)");
		
		result = dbinstance.Execute("create table student(stuno int, sname varchar,score double,course varchar)");
		
		result = dbinstance.Execute("create index in3 on student(sname,course)");
		
		result = dbinstance.Execute("create table student2(stuno3 int primary key, sname3 varchar,score3 double,course3 varchar)");
		
		result = dbinstance.Execute("create table teacher(stuno2 int primary key, score2 double,course2 varchar)");
		
		result = dbinstance.Execute("create table teacher2(stuno4 int primary key, score4 double,course4 varchar)");
		
		result = dbinstance.Execute("create table teacher3(stuno5 int primary key, score5 double,course5 varchar)");
		
		result = dbinstance.Execute("create table teacher4(stuno6 int primary key, score6 double,course6 varchar)");
		
		result = dbinstance.Execute("create table teacher5(stuno7 int primary key, score7 double,course7 varchar)");
	    
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对，update，select，delete都一样
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对，update，select，delete都一样
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		//写报告时写上经过测试没有where时不会有BeginFilter和EndFilter对
		
		//where空条件但含有where是语法错误，经过测试
		//where空条件但含有where是语法错误，经过测试
		//where空条件但含有where是语法错误，经过测试
		//where空条件但含有where是语法错误，经过测试
		//where空条件但含有where是语法错误，经过测试
		
		result = dbinstance.Execute("create index in on student(sname,course)");
		
		result = dbinstance.Execute("create index in2 on teacher(course2)");
		
		result = dbinstance.Execute("create index in on student(sname,course)");
		
		//这里11/7
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		//设置多次循环插入多条值
		
		
		/*String ss1="insert into student (stuno, sname, score, course) values (";
		String ss2,ss3="'lkili'",ss4,ss5="'kokj'";
		String ss6=")";
		Integer k;
		ss2="1000";
		ss4="1000";
		for(k=0;k<100;k++)
		{
			dbinstance.Execute(ss1+ss2+","+ss3+","+ss4+","+ss5+ss6);
		}*/
		
		//
		
		String s1="insert into student (stuno, sname, score, course) values (";
		String s2,s3="'lkili'",s4,s5="'kokj'";
		String s6=")";
		Integer i,j;
		for(i=0,j=100;i<100;i++,j++)
		{
			s2=i.toString();
			s4=j.toString();
			dbinstance.Execute(s1+s2+","+s3+","+s4+","+s5+s6);
		}
		dbinstance.Execute("select stuno,sname from student");
		
		result = dbinstance.Execute("insert into teacher (stuno2, sname2,score2, course2) values (2018, '李',90, 'Ddd')");
		//////////////
		//原因第四轮25被删掉后还有25的引用，错误解决
		//////////////
		result = dbinstance.Execute("delete from student where stuno>=2017 and sname='ll'");
		result = dbinstance.Execute("select course from student where sname=li");
		
		//result = dbinstance.Execute("select * from student");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017004, '了', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017009, '拉', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017005, '阿瑟东', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (20170, '麦克', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017, '迈克', 90, 'DS')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017005, 'la', 91, 'HY')");
	    
	    //result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017006, '李', 92, 'KS')");		
	    
	    //result = dbinstance.Execute("select * from student");
	    
	   	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017007, '张', 93, 'JS')");			   
	   	 /*第四轮最后一次写第44页*/ result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017008, '章', 94, 'HS')");	
	    /*11/7/1*/	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017009, '长', 95, 'NY')");
	    
	    //result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017010, '位', 96, 'BS')");		
	    
	    //result = dbinstance.Execute("select * from student");
	    
	   result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017011, '未', 97, 'AS')");
	    
	    
	    
	   /*第三轮25经历写出操作*/ result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017012, '魏', 90, 'DS')");		
	    

	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017013, 'la', 91, 'HY')");
	    
	    //result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017014, 'le', 92, 'KS')");		
	    

	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017015, 'lf', 93, 'JS')");		
	    
	    
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017016, 'lg', 94, 'HS')");		
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017017, 'lh', 95, 'NY')");
/*问题出在427行解决*/result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017018, 'lj', 96, 'BS')");		
/*就是这里的下一个出现2个行对应4个叶子节点，在上面*/	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017019, 'll', 97, 'AS')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017020, 'li', 90, 'DS')");		
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017021, 'la', 91, 'HY')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017022, 'le', 92, 'KS')");		
/*已经这里出现2个行对应4个叶子节点，在上面*/   result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017023, 'lf', 93, 'JS')");		
	    
	    
/*这里已经出现2个行对应4个叶子节点，在上面*/	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017024, 'lg', 94, 'HS')");		
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017025, 'lh', 95, 'NY')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017026, 'lj', 96, 'BS')");		
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017027, 'll', 97, 'AS')");
		
	    
	    
	    result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("select * from student where stuno=2017");
	    
		result = dbinstance.Execute("select * from student where sname='lh'");
		
		
	    result = dbinstance.Execute("select * from student where course='AS'");
		
		result = dbinstance.Execute("create index in on student(course)");	
		
		result = dbinstance.Execute("select * from student where course='AS'");
		
		result = dbinstance.Execute("delete from student where course='AS'");
		
	   	result = dbinstance.Execute("select * from student where course='AS'");
		
		result = dbinstance.Execute("select * from teacher where course2='Ddd'");
		
		result = dbinstance.Execute("delete from teacher where course2='Ddd'");
		
		result = dbinstance.Execute("select * from teacher where course2='Ddd'");
		
		
	    
		result = dbinstance.Execute("select * from student where sname='lh'");
		
		result = dbinstance.Execute("update student set score=666,stuno=98765 where sname='lh' and stuno>=95");
		
		result = dbinstance.Execute("select * from student where sname='lh'");
		
		result = dbinstance.Execute("select stuno2,stuno from student,teacher where stuno>=10000 or stuno2>=900");
		
		result = dbinstance.Execute("select * from student where score>800 and stuno>=2017087 or course<'AB' or score <= 10000 and score=909800");
		
		result = dbinstance.Execute("select * from student,teacher,student2 where stuno>=2017087 or stuno2>=900");
		
		//dump
		dbinstance.dump();
		
		//关闭
   		dbinstance.Close();
   		
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误，如果中途打断需要删除原文件
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
		//需要一次执行到DBInstance.close()，中途打断一些内存中的页会更新不到硬盘中导致第二次打开文件错误
   		////////////////////////////
   		//将当前表的状态写入文件，能够下次打开同一个文件，原先的程序并不支持打开同一个文件
   		////////////////////////////
	}
}
