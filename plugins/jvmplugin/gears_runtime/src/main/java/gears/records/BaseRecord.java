package gears.records;

import java.io.Serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Base record implementation.
 *
 */
public class BaseRecord implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BaseRecord() {}
	
	@Override
	public String toString() {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return "BaseRecord";
		}
	}
}
