package gears.readers;

public class CommandOverrider extends BaseReader<Object[]> {

	private String command;
	private String prefix;
	
	public CommandOverrider() {}
	
	public String getCommand() {
		return command;
	}

	public CommandOverrider setCommand(String command) {
		this.command = command;
		return this;
	}

	public String getPrefix() {
		return prefix;
	}

	public CommandOverrider setPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "CommandReader";
	}

}
