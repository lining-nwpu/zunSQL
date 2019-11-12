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
 * Hello world����Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
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
		//֧�ֶ�δ�ͬһ���ļ������test.db�Ѿ����ڣ���ᱣ��ԭ�еı�����֮���ټ�
		//֧�ֶ�δ�ͬһ���ļ�
		//֧�ֶ�δ�ͬһ���ļ�
		//֧�ֶ�δ�ͬһ���ļ�
		//֧�ֶ�δ�ͬһ���ļ�
		//֧�ֶ�δ�ͬһ���ļ�
		//֧�ֶ�δ�ͬһ���ļ�
		//֧�ֶ�δ�ͬһ���ļ�
		//////////////////////////////////////////////////////
		///////////////////////////////////////////////////////
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ����������;�����Ҫɾ��ԭ�ļ�
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		/////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////
		DBInstance dbinstance = DBInstance.Open("test.db", 5);
		QueryResult result;
		//dump
		dbinstance.dump();
		
		//��������5�ű�
		//��������5�ű�
		result = dbinstance.Execute("create table student(stuno int, sname varchar,score double,course varchar)");
		
		result = dbinstance.Execute("create table student(stuno int, sname varchar,score double,course varchar)");
		
		result = dbinstance.Execute("create index in3 on student(sname,course)");
		
		result = dbinstance.Execute("create table student2(stuno3 int primary key, sname3 varchar,score3 double,course3 varchar)");
		
		result = dbinstance.Execute("create table teacher(stuno2 int primary key, score2 double,course2 varchar)");
		
		result = dbinstance.Execute("create table teacher2(stuno4 int primary key, score4 double,course4 varchar)");
		
		result = dbinstance.Execute("create table teacher3(stuno5 int primary key, score5 double,course5 varchar)");
		
		result = dbinstance.Execute("create table teacher4(stuno6 int primary key, score6 double,course6 varchar)");
		
		result = dbinstance.Execute("create table teacher5(stuno7 int primary key, score7 double,course7 varchar)");
	    
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter�ԣ�update��select��delete��һ��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter�ԣ�update��select��delete��һ��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		//д����ʱд�Ͼ�������û��whereʱ������BeginFilter��EndFilter��
		
		//where������������where���﷨���󣬾�������
		//where������������where���﷨���󣬾�������
		//where������������where���﷨���󣬾�������
		//where������������where���﷨���󣬾�������
		//where������������where���﷨���󣬾�������
		
		result = dbinstance.Execute("create index in on student(sname,course)");
		
		result = dbinstance.Execute("create index in2 on teacher(course2)");
		
		result = dbinstance.Execute("create index in on student(sname,course)");
		
		//����11/7
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		//���ö��ѭ���������ֵ
		
		
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
		
		result = dbinstance.Execute("insert into teacher (stuno2, sname2,score2, course2) values (2018, '��',90, 'Ddd')");
		//////////////
		//ԭ�������25��ɾ������25�����ã�������
		//////////////
		result = dbinstance.Execute("delete from student where stuno>=2017 and sname='ll'");
		result = dbinstance.Execute("select course from student where sname=li");
		
		//result = dbinstance.Execute("select * from student");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017004, '��', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017009, '��', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017005, '��ɪ��', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (20170, '���', 90, 'DS')");
		result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017, '����', 90, 'DS')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017005, 'la', 91, 'HY')");
	    
	    //result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017006, '��', 92, 'KS')");		
	    
	    //result = dbinstance.Execute("select * from student");
	    
	   	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017007, '��', 93, 'JS')");			   
	   	 /*���������һ��д��44ҳ*/ result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017008, '��', 94, 'HS')");	
	    /*11/7/1*/	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017009, '��', 95, 'NY')");
	    
	    //result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017010, 'λ', 96, 'BS')");		
	    
	    //result = dbinstance.Execute("select * from student");
	    
	   result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017011, 'δ', 97, 'AS')");
	    
	    
	    
	   /*������25����д������*/ result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017012, 'κ', 90, 'DS')");		
	    

	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017013, 'la', 91, 'HY')");
	    
	    //result = dbinstance.Execute("select * from student");
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017014, 'le', 92, 'KS')");		
	    

	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017015, 'lf', 93, 'JS')");		
	    
	    
	    
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017016, 'lg', 94, 'HS')");		
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017017, 'lh', 95, 'NY')");
/*�������427�н��*/result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017018, 'lj', 96, 'BS')");		
/*�����������һ������2���ж�Ӧ4��Ҷ�ӽڵ㣬������*/	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017019, 'll', 97, 'AS')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017020, 'li', 90, 'DS')");		
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017021, 'la', 91, 'HY')");
	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017022, 'le', 92, 'KS')");		
/*�Ѿ��������2���ж�Ӧ4��Ҷ�ӽڵ㣬������*/   result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017023, 'lf', 93, 'JS')");		
	    
	    
/*�����Ѿ�����2���ж�Ӧ4��Ҷ�ӽڵ㣬������*/	    result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017024, 'lg', 94, 'HS')");		
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
		
		//�ر�
   		dbinstance.Close();
   		
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ����������;�����Ҫɾ��ԭ�ļ�
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
		//��Ҫһ��ִ�е�DBInstance.close()����;���һЩ�ڴ��е�ҳ����²���Ӳ���е��µڶ��δ��ļ�����
   		////////////////////////////
   		//����ǰ���״̬д���ļ����ܹ��´δ�ͬһ���ļ���ԭ�ȵĳ��򲢲�֧�ִ�ͬһ���ļ�
   		////////////////////////////
	}
}
