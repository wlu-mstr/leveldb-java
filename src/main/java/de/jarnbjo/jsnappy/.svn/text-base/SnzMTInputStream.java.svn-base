package de.jarnbjo.jsnappy;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Alternative to <code>SnzInputStream</code> with support for multi-threaded decomression.
 * </p>
 *
 * <p>
 * By default, SnzMTInputStream will use a default ThreadPoolExecutor with a core size
 * set to the number of available CPUs and a maximum size of twice the number of available
 * CPUs. The threads used yby the default executor will be started as daemon threads.
 * The default executor is not created until actually requried and can be queried and
 * replaced with the static <code>getDefaultExecutor</code> and <code>setDefaultExecutor</code>
 * methods.
 * </p>
 *
 * @author Tor-Einar Jarnbjo
 * @since 1.0
 */
public class SnzMTInputStream extends SnzInputStream {

	private static final int AVAILABLE_CPUS = Runtime.getRuntime().availableProcessors();
	private static Executor defaultExecutor = null;

	private boolean meEof = false, delegateEof = false;

	private LinkedList<DecompTask> tasks = new LinkedList<DecompTask>();

	private Buffer dbuffer;
	private int dbufferIndex = 0;

	private Executor executor;

	/**
	 * Creates an input stream, which reads from the specified input
	 * and uses the default executor.
	 * @param in
	 */
	public SnzMTInputStream(InputStream in) {
		this(in, initDefaultExecutor());
	}

	/**
	 * Creates an input stream, which reads from the specified input
	 * and uses the provided executor instead of the default executor.
	 * @param in
	 * @param executor
	 */
	public SnzMTInputStream(InputStream in, Executor executor) {
		super(in);
		this.executor = executor;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {

		init();

		if (meEof) {
			return -1;
		}

		if (dbuffer == null || dbufferIndex >= dbuffer.getLength()) {

			if(tasks.size() == 0) {
				meEof = true;
				return -1;
			}

			DecompTask task = tasks.removeFirst();
			try {
				dbuffer = task.get();
			}
			catch(Exception e) {
				throw new IOException(e.getMessage());
			}
			dbufferIndex = 0;
			newChunk();
		}

		if(len > dbuffer.getLength() - dbufferIndex) {
			len = dbuffer.getLength() - dbufferIndex;
		}

		System.arraycopy(dbuffer.getData(), dbufferIndex, b, off, len);
		dbufferIndex += len;

		return len;
	}

	/**
	 * Returns the default executor or null if no default executor has
	 * been created yet.
	 * @return the default executor
	 */
	public static Executor getDefaultExecutor() {
		return defaultExecutor;
	}

	/**
	 * <p>
	 * Sets a new default executor.
	 * </p>
	 *
	 * <p>
	 * Setting a new default executor will have no effect on already created
	 * instances of this class. Setting the default executor to null will cause
	 * a new default executor to be created when needed.
	 * </p>
	 * 
	 * <p>
	 * Note that replacing a running executor will not cause the previous default
	 * executor to be automatically shut down.
	 * </p>
	 * @param executor a new default executor
	 */
	public static synchronized void setDefaultExecutor(Executor executor) {
		defaultExecutor = executor;
	}

	private static synchronized Executor initDefaultExecutor() {
		if(defaultExecutor == null) {
			defaultExecutor =
				new ThreadPoolExecutor(AVAILABLE_CPUS, AVAILABLE_CPUS * 2, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
						new ThreadFactory() {
							private int cnt = 1;
							public Thread newThread(Runnable r) {
								Thread t = new Thread(r, "SnzMTInputStream-" + (cnt++));
								t.setDaemon(true);
								return t;
							}
				});
		}
		return defaultExecutor;
	}

	private void newChunk() throws IOException {
		if(!delegateEof) {
			int cLength = readVInt();
			if (cLength == 0) {
				delegateEof = true;
				return;
			}
			byte[] cbuffer = new byte[cLength];
			int o = 0;
			while(o < cLength) {
				o += in.read(cbuffer, o, cbuffer.length - o);
			}
			DecompTask task = new DecompTask(cbuffer);
			tasks.add(task);
			executor.execute(task);
		}
	}

	void init() throws IOException {
		if(!initialized) {
			super.init();
			for(int i=0; i<AVAILABLE_CPUS*2; i++) {
				newChunk();
			}
		}
	}

	private class DecompTask extends FutureTask<Buffer> {

		public DecompTask(byte[] source) {
			this(source, new Buffer());
		}

		private DecompTask(final byte[] source, final Buffer result) {
			super(new Runnable() {
				public void run() {
					SnappyDecompressor.decompress(source, result);
				}
			}, result);
		}

	}

}
