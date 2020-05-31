package gears.records;

public class StringRecord extends BaseRecord {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String val;
	
	public StringRecord(String s) {
		super();
		this.val = s;
	}
	
	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}
	
	@Override
	public String toString() {
		return val;
	}
}
