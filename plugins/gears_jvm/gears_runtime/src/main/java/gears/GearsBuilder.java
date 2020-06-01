package gears;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutputStream;

import gears.operations.AccumulateByOperation;
import gears.operations.AccumulateOperation;
import gears.operations.ExtractorOperation;
import gears.operations.FilterOperation;
import gears.operations.ForeachOperation;
import gears.operations.MapOperation;
import gears.readers.BaseReader;

public class GearsBuilder{
	
	private BaseReader reader;
	private long ptr;
	
	private native void Init(String reader);
	
	private native void Destroy();
	
	public native GearsBuilder Map(MapOperation mapper);
	
	public native GearsBuilder FlatMap(MapOperation mapper);
	
	public native GearsBuilder Foreach(ForeachOperation foreach);
	
	public native GearsBuilder Filter(FilterOperation foreach);
	
	public native GearsBuilder AccumulateBy(ExtractorOperation extractor, AccumulateByOperation accumulator);
	
	public native GearsBuilder LocalAccumulateBy(ExtractorOperation extractor, AccumulateByOperation accumulator);
	
	public native GearsBuilder Accumulate(AccumulateOperation accumulator);
	
	public native GearsBuilder Collect();
	
	public native GearsBuilder Repartition(ExtractorOperation extractor);
	
	private native void InnerRun(BaseReader reader);
	
	private native void InnerRegister(BaseReader reader);
	
	public void Run() {
		InnerRun(reader);
	}
	
	public void Register() {
		InnerRegister(reader);
	}
	
	public GearsBuilder(BaseReader reader) {
		this.reader = reader;
		Init(reader.GetName());
	}
	
	public static byte[] SerializeObject(Object o) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bos);
		out.writeObject(o);
		out.flush();
		byte[] bytes = bos.toByteArray();
		bos.close();
		return bytes;
	}
	
	public static Object DeserializeObject(byte[] bytes, ClassLoader cl) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = new ObjectInputStreamWithLoader(bis, cl);;
		Object o = in.readObject();
		in.close();
		return o;
	}
	
	@Override
	protected void finalize() throws Throwable {
		Destroy();
	}
}
