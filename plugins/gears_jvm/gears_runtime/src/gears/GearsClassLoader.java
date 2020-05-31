package gears;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class GearsClassLoader {
	public static URLClassLoader GetNew(String jarFilePath) throws MalformedURLException, FileNotFoundException {
		File f = new File(jarFilePath);
		if(!f.exists()) {
			throw new FileNotFoundException(jarFilePath + " not exists");
		}
		URLClassLoader l = new URLClassLoader(new URL[] {f.toURI().toURL()}, GearsClassLoader.class.getClassLoader());
		return l;
	}
}
