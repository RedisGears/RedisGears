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
	
	public void add(BaseRecord r) {
		recordList.add(r);
	}
	
	public BaseRecord get(int index) {
		return recordList.get(index);
	}
	
	public int len() {
		return recordList.size();
	}
}
