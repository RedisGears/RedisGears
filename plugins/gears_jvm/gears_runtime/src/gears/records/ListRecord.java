package gears.records;

import java.util.ArrayList;
import java.util.List;

public class ListRecord extends BaseRecord {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private List<BaseRecord> recordList;
	
	public ListRecord() {
		recordList = new ArrayList<BaseRecord>();
	}
	
	public void Add(BaseRecord r) {
		recordList.add(r);
	}
	
	public BaseRecord Get(int index) {
		return recordList.get(index);
	}
	
	public int Len() {
		return recordList.size();
	}
}
