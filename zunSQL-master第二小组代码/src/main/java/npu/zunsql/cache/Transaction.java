package npu.zunsql.cache;

//import javax.print.DocFlavor;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

public class Transaction
{
    protected static int transCount = 0;
    protected int transID;

    protected boolean WR;

    protected ReadWriteLock lock;

    public Transaction(String s, ReadWriteLock lock)
    {
        if(s == "r")
            this.WR = false;
        else
            this.WR = true;
        this.lock = lock;
        this.transID=0;
        //this.transID = transCount++;
    }

    public void begin()
    {
        /*if(this.WR)
        {
            this.lock.writeLock().lock();
        }
        else {
            this.lock.readLock().lock();
        }*/
    }
    
    public void commit()
    {
        if(this.WR)
        {
            this.lock.writeLock().unlock();
        }
        else
        {
            this.lock.readLock().unlock();
        }
    }
}
