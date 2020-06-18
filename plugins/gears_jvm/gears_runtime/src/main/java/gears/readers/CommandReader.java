package gears.readers;

public class CommandReader extends BaseReader<Object[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String trigger;

	public CommandReader() {
		this.setTrigger(null);
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "CommandReader";
	}

	public CommandReader setTrigger(String trigger) {
		this.trigger = trigger;
		return this;
	}

	public String getTrigger() {
		return trigger;
	}
	
	
}
