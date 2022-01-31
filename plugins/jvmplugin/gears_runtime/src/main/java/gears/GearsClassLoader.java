package gears;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Internal use
 * 
 */
public class GearsClassLoader extends URLClassLoader{
	long ptr;
	
	public GearsClassLoader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
	}
	
	public void shutDown() {
	}
	
	@Override
	protected void finalize() throws Throwable {
		GearsBuilder.classLoaderFinalized(ptr);
	}
	
	public static URLClassLoader getNew(String jarFilePath) throws MalformedURLException, FileNotFoundException {
		File f = new File(jarFilePath);
		if(!f.exists()) {
			throw new FileNotFoundException(jarFilePath + " not exists");
		}
		return new GearsClassLoader(new URL[] {f.toURI().toURL()}, GearsClassLoader.class.getClassLoader());
	}
}
