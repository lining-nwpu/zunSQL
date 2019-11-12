package npu.zunsql.treemng;

import npu.zunsql.cache.CacheMgr;

import java.io.IOException;

/**
 * Created by Ed on 2017/10/28.
 */
public abstract class Transaction 
{
	public Integer tranNum=0;

	public CacheMgr cacheMagr;

	protected Transaction(Integer number, CacheMgr thisCacheMgr) {
		tranNum = 0;//事务号总是0
		cacheMagr = thisCacheMgr;
	}

	public abstract boolean Commit() throws IOException;

}

class WriteTran extends Transaction
{
	protected WriteTran(int num, CacheMgr thisCacheMgr) 
	{
		super(num, thisCacheMgr);
	}

	public boolean Commit() throws IOException 
	{
//		System.out.print(tranNum);
		cacheMagr.commitTransation(tranNum);
		return true;
	}
}

class ReadTran extends Transaction 
{
	protected ReadTran(int num, CacheMgr thisCacheMgr) 
	{
		super(num, thisCacheMgr);
	}

	public boolean Commit() 
	{
		try {
			cacheMagr.commitTransation(tranNum);
		} 
		catch (IOException e) 
		{
			return false;
		}
		return true;
	}
}

class UserTran extends Transaction 
{
	protected UserTran(int num, CacheMgr thisCacheMgr) 
	{
		super(num, thisCacheMgr);
	}

	public boolean Commit() 
	{
		try {
			cacheMagr.commitUserTransation(tranNum);
		} 
		catch (IOException e) 
		{
			return false;
		}
		return true;
	}
}