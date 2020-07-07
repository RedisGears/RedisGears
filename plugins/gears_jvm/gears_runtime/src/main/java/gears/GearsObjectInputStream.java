package gears;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.StreamCorruptedException;

/**
 * Internal use
 *
 */
class GearsObjectInputStream
{
	private class LoaderObjectInputStream extends ObjectInputStream{

		private ClassLoader loader;
		
		public LoaderObjectInputStream(ClassLoader loader, GearsByteInputStream in) throws IOException {
			super(in);
			// TODO Auto-generated constructor stub
			this.loader = loader;
		}
		
		@Override
		protected void readStreamHeader() {
			
		}
		
		/**
	     * Use the given ClassLoader rather than using the system class
	     * @throws ClassNotFoundException 
	     * @throws IOException 
	     */
	    @SuppressWarnings({ "rawtypes", "unchecked" })
	    @Override
	    protected Class resolveClass(ObjectStreamClass classDesc) throws ClassNotFoundException, IOException {
	    	if (loader == null)
	    		return super.resolveClass(classDesc);
		    try {
		    	return Class.forName(classDesc.getName(), false, loader);
		    } catch (ClassNotFoundException ex) {
		    	Class<?> cl = super.resolveClass(classDesc); // fall back for resolving primitive types
		    	if (cl != null)
		    		return cl;
		    	throw ex;
		    }
	    }
	}
	
    private ClassLoader loader;
    private GearsByteInputStream in;
    private LoaderObjectInputStream objectIn;

    /**
     * Loader must be non-null;
     */

    public GearsObjectInputStream(ClassLoader loader, GearsByteInputStream in) {
        this.in = in;
        if (loader == null) {
            throw new IllegalArgumentException("Illegal null argument to ObjectInputStreamWithLoader");
        }
        this.loader = loader;
        this.objectIn = null;
    }
    
    public ClassLoader getLoader() {
    	return loader;
    }
     
    public Object readObject() throws ClassNotFoundException, IOException {
    	if(objectIn == null) {
    		objectIn = new LoaderObjectInputStream(loader, in);
    	}
    	return objectIn.readObject();
    }
    
    public void addData(byte[] bytes) {
    	in.addData(bytes);
    }
    
    public static GearsObjectInputStream getGearsObjectInputStream(ClassLoader loader) throws IOException {
    	GearsByteInputStream in = new GearsByteInputStream();
    	return new GearsObjectInputStream(loader, in);
    }
}

