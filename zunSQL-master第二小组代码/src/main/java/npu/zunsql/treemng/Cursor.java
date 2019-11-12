package npu.zunsql.treemng;

import npu.zunsql.cache.Page;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/28.
 */
public abstract class Cursor {
	protected Cursor() {
		;
	}
	
	//鍒ゆ柇鏄惁涓虹┖
	public abstract boolean isEmpty();

	// 鑾峰彇鍒楃被鍨�
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public abstract BasicType getColumnType(String columnName);

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓瀛楃涓层��
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public abstract String getCell_s(String columnName);

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓鏁村舰銆�
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public abstract Integer getCell_i(String columnName);

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓鍙岀簿搴︺��
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public abstract Double getCell_d(String columnName);

	// 鑾峰彇涓婚敭鍗曞厓瀛楃涓层��
	public abstract String getKeyCell_s();

	// 鑾峰彇涓婚敭鍗曞厓鏁村舰銆�
	public abstract Integer getKeyCell_i();

	// 鑾峰彇涓婚敭鍗曞厓鍙岀簿搴︺��
	public abstract Double getKeyCell_d();

	// 娓告爣绉昏嚦棣栨潯
	public abstract boolean moveToFirst(Transaction thisTran) throws IOException, ClassNotFoundException;

	// 娓告爣绉昏嚦鏈熬
	public abstract boolean moveToLast(Transaction thisTran) throws IOException, ClassNotFoundException;

	// 娓告爣鍚庣Щ涓�鏉�
	public abstract boolean moveToNext(Transaction thisTran) throws IOException, ClassNotFoundException;

	// 娓告爣鍓嶇Щ涓�鏉�
	public abstract boolean moveToPrevious(Transaction thisTran) throws IOException, ClassNotFoundException;

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勫瓧绗︿覆鍊�
	//public abstract boolean moveToUnpacked(Transaction thisTran, String key) throws IOException, ClassNotFoundException;

//	public abstract boolean moveToUnpacked(Transaction thisTran, Integer key)
//			throws IOException, ClassNotFoundException;

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勫弻绮惧害鍊�
//	public abstract boolean moveToUnpacked(Transaction thisTran, Double key) throws IOException, ClassNotFoundException;

	// 鍒犻櫎鏈潯
	public abstract boolean delete(Transaction thistran) throws IOException, ClassNotFoundException;

	// 鎻掑叆涓�鏉�
	public abstract boolean insert(Transaction thisTran, List<String> stringList)
			throws IOException, ClassNotFoundException;

	// 鑾峰彇鏈潯鍐呭锛屽瓧绗︿覆鍊�
	public abstract List<String> getData();

	// 璋冩暣鏈潯鍐呭
	public abstract boolean setData(Transaction thisTran, List<String> stringList)
			throws IOException, ClassNotFoundException;
}

class TableCursor extends Cursor {
	protected Table aimTable;
	protected int thisRowID;
	protected Node thisNode;

	protected TableCursor(Table thisTable, Transaction thisTran) throws IOException, ClassNotFoundException {
		super();
		aimTable = thisTable;
		thisRowID = 0;
		thisNode = aimTable.getRootNode(thisTran);
	}
	
	public boolean isEmpty() {
		return thisNode == null;
	}

	public BasicType getColumnType(String columnName) {
		return aimTable.getColumn(columnName).getType();
	}

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓瀛楃涓层��
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public String getCell_s(String columnName) {
		return thisNode.getRow(thisRowID).getCell(aimTable.getColumn(columnName).getNumber()).getValue_s();
	}

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓鏁村舰銆�
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public Integer getCell_i(String columnName) {
//		if (aimTable.getColumn(columnName).getNumber() == 2) {
//			int a = 0;
//		}
		return thisNode.getRow(thisRowID).getCell(aimTable.getColumn(columnName).getNumber()).getValue_i();
	}

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓鍙岀簿搴︺��
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public Double getCell_d(String columnName) {
		return thisNode.getRow(thisRowID).getCell(aimTable.getColumn(columnName).getNumber()).getValue_d();
	}

	// 鑾峰彇涓婚敭鍗曞厓瀛楃涓层��
	public String getKeyCell_s() {
		return getCell_s(aimTable.getKeyColumn().getName());
	}

	// 鑾峰彇涓婚敭鍗曞厓鏁村舰銆�
	public Integer getKeyCell_i() {
		return getCell_i(aimTable.getKeyColumn().getName());
	}

	// 鑾峰彇涓婚敭鍗曞厓鍙岀簿搴︺��
	public Double getKeyCell_d() {
		return getCell_d(aimTable.getKeyColumn().getName());
	}

	// 娓告爣绉昏嚦棣栨潯
	public boolean moveToFirst(Transaction thisTran) throws IOException, ClassNotFoundException {

		thisNode = aimTable.getRootNode(thisTran);

		if (thisNode == null) {
			return false;
		}

		while (thisNode.getSonNodeList() != null) {
			thisNode = thisNode.getSpecialSonNode(0, thisTran);
		}

		thisRowID = 0;
		return true;
	}

	// 娓告爣绉昏嚦鏈熬
	public boolean moveToLast(Transaction thisTran) throws IOException, ClassNotFoundException {
		// thisNode = aimTable.getRootNode(thisTran);
		while (thisNode.getSonNodeList().size() != 0) {
			thisNode = thisNode.getSpecialSonNode(thisNode.getSonNodeList().size() - 1, thisTran);
		}

		return true;
	}

	// 娓告爣鍚庣Щ涓�鏉�
	public boolean moveToNext(Transaction thisTran) throws IOException, ClassNotFoundException {
		if (thisNode == null) {
			return false;
		}
		// int flagchang=0;
		if (thisRowID < thisNode.getRowList().size() - 1) {
			thisRowID++;
			return true;
		} else {
			while (thisNode.getFatherNodeID() > 0) {
				if (thisNode.getOrder() < thisNode.getFatherNode(thisTran).getSonNodeList().size() - 1) {
					thisNode = thisNode.getFatherNode(thisTran).getSpecialSonNode(thisNode.getOrder() + 1, thisTran);
					while ((thisNode.getSonNodeList() != null) && (thisNode.getSonNodeList().size() != 0)) {
						thisNode = thisNode.getSpecialSonNode(0, thisTran);
					}
					thisRowID = 0;
					return true;
				} else // 濡傛灉褰撳墠缁撶偣鐨勭埗浜茬粨鐐逛綅鏄綋鍓嶇粨鐐圭殑绁栫埗缁撶偣鐨勬渶鍚庝竴涓効瀛愮粨鐐�
				{
					thisNode = thisNode.getFatherNode(thisTran);

				}
			}
		}
		moveToLast(thisTran);
//		thisNode = null;
		return false;
	}

	// 娓告爣鍓嶇Щ涓�鏉�
	public boolean moveToPrevious(Transaction thisTran) throws IOException, ClassNotFoundException {
		if (thisRowID > 0) {
			thisRowID--;
			return true;
		} else {
			while (thisNode.getFatherNodeID() > 0) {
				if (thisNode.getOrder() > 0) {
					thisNode = thisNode.getFatherNode(thisTran).getSpecialSonNode(thisNode.getOrder() - 1, thisTran);
					while ((thisNode.getSonNodeList() != null) && (thisNode.getSonNodeList().size() != 0)) {
						thisNode = thisNode.getSpecialSonNode(thisNode.getSonNodeList().size() - 1, thisTran);
					}
					thisRowID = thisNode.getRowList().size() - 1;
					return true;
				} else // 濡傛灉褰撳墠缁撶偣鐨勭埗浜茬粨鐐逛綅鏄綋鍓嶇粨鐐圭殑绁栫埗缁撶偣鐨勭涓�涓効瀛愮粨鐐�
				{
					thisNode = thisNode.getFatherNode(thisTran);

				}
			}
		}
		moveToFirst(thisTran);
		return false;
	}

	private boolean moveToSon(Transaction thisTran, Cell key) throws IOException, ClassNotFoundException {
		if ((thisNode.getRowList() == null) || (thisNode.getRowList().size() == 0)) 
		{
			return false;
		}

		for (int i = 0; i < thisNode.getRowList().size(); i++) 
		{
			if (key.equalTo(thisNode.getRowList().get(i).getCell(aimTable.keyColumn.getNumber()))) 
			{
				thisRowID = i;
				return true;
			}
		}

		if ((thisNode.getSonNodeList() == null) || (thisNode.getSonNodeList().size() == 0)) 
		{
			return false;
		}

		for (int i = 0; i < thisNode.getRowList().size(); i++) {
			if (thisNode.getRowList().get(i).getCell(0).bigerThan(key)) {
				thisNode = thisNode.getSpecialSonNode(i, thisTran);
				return moveToSon(thisTran, key);
			}
		}

		if (key.bigerThan(thisNode.getRowList().get(thisNode.getRowList().size() - 1).getCell(0))) {
			thisNode = thisNode.getSpecialSonNode(thisNode.getRowList().size(), thisTran);
			return moveToSon(thisTran, key);
		}
		return false;
	}

//	public boolean moveToUnpacked(Transaction thisTran, String key) throws IOException, ClassNotFoundException {
//		if (aimTable.rootNodePage == -1) 
//		{
//			return false;
//		}
//		//thisNode = aimTable.getRootNode(thisTran);//可以删掉的
//		//thisRowID = 0;可以删掉的
//		moveToSon(thisTran, new Cell(key));
//		return true;
//	}

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勬暣鍨嬪��
	public boolean moveToUnpacked(Transaction thisTran, Integer key) throws IOException, ClassNotFoundException {
		thisNode = aimTable.getRootNode(thisTran);
		thisRowID = 0;
		moveToSon(thisTran, new Cell(key.toString()));
		return true;
	}

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勫弻绮惧害鍊�
	public boolean moveToUnpacked(Transaction thisTran, Double key) throws IOException, ClassNotFoundException {
		thisNode = aimTable.getRootNode(thisTran);
		thisRowID = 0;
		moveToSon(thisTran, new Cell(key.toString()));
		return true;
	}

	// 鍒犻櫎鏈潯
	public boolean delete(Transaction thisTran) throws IOException, ClassNotFoundException {
		int i=0;
		Cell keyCell = null;
		if (thisNode == null) 
		{
			return false;
		} 
		else {
			Page ppp=aimTable.cacheManager.readPage(thisTran.tranNum, thisNode.pageOne);
			ByteBuffer thisBufer = ppp.getPageBuffer();
	        byte [] bytes=new byte[Page.PAGE_SIZE] ;
	        thisBufer.rewind();
	        thisBufer.get(bytes,0,thisBufer.remaining());

	        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
	        ObjectInputStream objTable=new ObjectInputStream(byteTable);
	        
	        String s1;
	        List<String>s2;
	        objTable.readObject();
	        s1=(String)objTable.readObject();
	        s2=(List<String>)objTable.readObject();
	        for(String s3:s2)
	        {
	        	if(s3.contentEquals(s1))
	        	{
	        		break;
	        	}
	        	else {
	        		i++;
	        	}
	        }
	        
			keyCell = thisNode.getRow(thisRowID).getCell(i);
		}

		moveToNext(thisTran);

		Cell nextCell = null;
		if (thisNode != null) {
			nextCell = thisNode.getRow(thisRowID).getCell(0);
		} else {
			nextCell = null;
		}

		aimTable.getRootNode(thisTran).deleteRow(keyCell, i,thisTran);
		Node rootNode = aimTable.getRootNode(thisTran);
		
		if(rootNode.getFirstRow(thisTran) == null) {
			thisNode = null;
		}

		if (thisNode != null) {
			//moveToUnpacked(thisTran, nextCell.getValue_s());
		}
		return true;
	}

	public boolean insert(Transaction thisTran, List<String> stringList) throws IOException, ClassNotFoundException {
		Row row = new Row(stringList);
		/*if (aimTable.rootNodePage == -1) 
		{
			ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
			byte[] bytes = new byte[Page.PAGE_SIZE];
			ByteArrayOutputStream byt = new ByteArrayOutputStream();
			ObjectOutputStream obj = new ObjectOutputStream(byt);
			List<Node> sonList = new ArrayList<Node>();
			obj.writeObject(sonList);
			obj.writeObject(-1);
			obj.writeObject(-1);
			List<Row> rowList = new ArrayList<Row>();
			rowList.add(row);
			obj.writeObject(rowList);
			bytes = byt.toByteArray();
			tempBuffer.put(bytes);
			Page rootPage = new Page(tempBuffer);
			aimTable.cacheManager.writePage(thisTran.tranNum, rootPage);
			aimTable.writeRootNodePage(rootPage.getPageID(), thisTran);
			Node rootNode = new Node(rootPage.getPageID(), aimTable.cacheManager, thisTran);
			int a = 0;
			// rootNode.insertRow(row,thisTran);
		}*/
		row=new Row(stringList);
		aimTable.getRootNode(thisTran).insertRow(row, thisTran);
		return true;
		//不需要的游标移动//return moveToUnpacked(thisTran, row.getCell(aimTable.keyColumn.getNumber()).getValue_s());
	}

	public List<String> getData() {
		if (thisNode == null) {
			return new ArrayList<String>();
		}
		Row row = thisNode.getRow(thisRowID);
		if (row == null) {
			return new ArrayList<String>();
		} else {
			return row.getStringList();
		}
//		return thisNode.getRow(thisRowID).getStringList();
	}

	// 璋冩暣鏈潯鍐呭
	public boolean setData(Transaction thistran, List<String> stringList) throws IOException, ClassNotFoundException {
		if (!delete(thistran)) {
			return false;
		}
		;
		if (!insert(thistran, stringList)) {
			return false;
		}
		;
		return true;
	}
}

class ViewCursor extends Cursor {
	protected View aimView;
	protected int RowID;

	protected ViewCursor(View aView) {
		super();
		aimView = aView;
		RowID = 0;
	}
	
	public boolean isEmpty() {
		return aimView == null;
	}
	
	// 鑾峰彇鍒楃被鍨�
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public BasicType getColumnType(String columnName) {
		return aimView.getColumn(columnName).getType();
	}

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓瀛楃涓层��
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public String getCell_s(String columnName) {
		return aimView.rowList.get(RowID).getCell(aimView.getColumn(columnName).getNumber()).getValue_s();
	}

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓鏁村舰銆�
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public Integer getCell_i(String columnName) {
		return aimView.rowList.get(RowID).getCell(aimView.getColumn(columnName).getNumber()).getValue_i();
	}

	// 鑾峰彇鏌愪竴鍒楃殑鍗曞厓鍙岀簿搴︺��
	// 杈撳叆鍙傛暟锛歝olumnName锛屽垪鍚嶃��
	public Double getCell_d(String columnName) {
		return aimView.rowList.get(RowID).getCell(aimView.getColumn(columnName).getNumber()).getValue_d();
	}

	// 鑾峰彇绗竴鍒楀崟鍏冨瓧绗︿覆銆�
	public String getKeyCell_s() {
		return aimView.rowList.get(RowID).getCell(0).getValue_s();
	}

	// 鑾峰彇绗竴鍒楀崟鍏冩暣褰€��
	public Integer getKeyCell_i() {
		return aimView.rowList.get(RowID).getCell(0).getValue_i();
	}

	// 鑾峰彇绗竴鍒楀崟鍏冨弻绮惧害銆�
	public Double getKeyCell_d() {
		return aimView.rowList.get(RowID).getCell(0).getValue_d();
	}

	// 娓告爣绉昏嚦棣栨潯
	public boolean moveToFirst(Transaction thisTran) {
		RowID = 0;
		return true;
	}

	// 娓告爣绉昏嚦鏈熬
	public boolean moveToLast(Transaction thisTran) {
		RowID = aimView.rowList.size() - 1;
		return true;
	}

	// 娓告爣鍚庣Щ涓�鏉�
	public boolean moveToNext(Transaction thisTran) {
		if (RowID < aimView.rowList.size() - 1) {
			RowID++;
			return true;
		} else {
			return false;
		}
	}

	// 娓告爣鍓嶇Щ涓�鏉�
	public boolean moveToPrevious(Transaction thisTran) {
		if (RowID > 0) {
			RowID--;
			return true;
		} else {
			return false;
		}
	}

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勫瓧绗︿覆鍊�
	public boolean moveToUnpacked(Transaction thisTran, String key) {
		return false;
	}

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勬暣鍨嬪��
	public boolean moveToUnpacked(Transaction thisTran, Integer key) {
		return false;
	}

	// 娓告爣绉昏嚦鎸囧畾浣�
	// 杈撳叆鍙傛暟锛歬ey涓婚敭鐨勫弻绮惧害鍊�
	public boolean moveToUnpacked(Transaction thisTran, Double key) {
		return false;
	}

	// 鍒犻櫎鏈潯
	public boolean delete(Transaction thistran) {
		return false;
	}

	// 鎻掑叆涓�鏉�
	public boolean insert(Transaction thisTran, List<String> stringList) {
		return false;
	}

	// 鑾峰彇鏈潯鍐呭锛屽瓧绗︿覆鍊�
	public List<String> getData() {
		return aimView.rowList.get(RowID).getStringList();
	}

	// 璋冩暣鏈潯鍐呭
	public boolean setData(Transaction thisTran, List<String> stringList) {
		if (!delete(thisTran)) {
			return false;
		}
		if (!insert(thisTran, stringList)) {
			return false;
		}
		return true;
	}
}