package gears;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.StreamCorruptedException;

class ObjectInputStreamWithLoader extends ObjectInputStream
{
    private ClassLoader loader;

    /**
     * Loader must be non-null;
     */

    public ObjectInputStreamWithLoader(InputStream in, ClassLoader loader)
            throws IOException, StreamCorruptedException {

        super(in);
        if (loader == null) {
            throw new IllegalArgumentException("Illegal null argument to ObjectInputStreamWithLoader");
        }
        this.loader = loader;
    }

    /**
     * Use the given ClassLoader rather than using the system class
     * @throws ClassNotFoundException 
     * @throws IOException 
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
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

