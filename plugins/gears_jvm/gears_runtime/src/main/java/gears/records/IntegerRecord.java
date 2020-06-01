package gears.records;

public class IntegerRecord extends BaseRecord {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int val;
	
	public IntegerRecord(int v) {
		val = v;
	}
	
	public int getVal() {
		return val;
	}

	public void setVal(int val) {
		this.val = val;
	}
	
	@Override
	public String toString() {
		return Integer.toString(val);
	}
}
