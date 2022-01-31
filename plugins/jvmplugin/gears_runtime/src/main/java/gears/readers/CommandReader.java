package gears.readers;

/**
 * A command reader implementation, trigger an execution on command.
 * Command usage:
 * 		RG.TRIGGER <Trigger name> [arg1 arg2 ...]
 *
 */
public class CommandReader extends BaseReader<Object[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String trigger;

	/**
	 * Creates a new command reader
	 */
	public CommandReader() {
		this.setTrigger(null);
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "CommandReader";
	}

	/**
	 * Set the trigger name that will trigger the execution
	 * @param trigger the trigger name that will trigger the execution
	 * @return the reader
	 */
	public CommandReader setTrigger(String trigger) {
		this.trigger = trigger;
		return this;
	}

	/**
	 * Returns the trigger name that will trigger the execution.
	 * @return the trigger name that will trigger the execution.
	 */
	public String getTrigger() {
		return trigger;
	}
	
	
}
