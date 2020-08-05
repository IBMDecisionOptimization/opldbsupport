//   Copyright 2020 IBM Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
package ilog.opl.dbsupport;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import ilog.concert.IloException;
import ilog.opl.IloOplElement;
import ilog.opl.IloOplElementDefinition;
import ilog.opl.IloOplFactory;
import ilog.opl.IloOplModel;
import ilog.opl.externaldata.DataConnection;
import ilog.opl.externaldata.DataExporter;
import ilog.opl.externaldata.DataImporter;
import ilog.opl.externaldata.InputRowIterator;
import ilog.opl.externaldata.OutputRowIterator;

/** Custom data handler that handles input from and output to databases.
 * The constructor of this class will automatically register data input and output handlers
 * with the passed {@link IloOplModel} that allow input and output similar to what used to
 * be possible with DbConnection, DbRead, DbWrite, DbExecute.
 *
 * One argument to the constructor is a <code>PREFIX</code>. With this string, the following
 * statements become valid in a .dat file:
 * <pre>
 *    PREFIXConnection conn(connstr, extra);
 *    i from PREFIXRead(conn, query);
 *    r to PREFIXPublish(conn, update);
 * </pre>
 * In this code segment
 * - <code>conn</code> is the identifier for a connection (that is created in the first line)
 * - <code>connstr</code> is a string that is passed as connection string to JDBC
 * - <code>extra</code> is extra information that is handled differently by different drivers.
 *   For example, for {@link ilog.opl.externaldata.jdbc.JdbcConnection} it specifies extra SQL commands (separated by semicolon)
 *   that are executed before the first <b>write</b> to the connection (if there are no writes
 *   they will not be executed).
 * - <code>query</code> describes how to read data (for {@link ilog.opl.externaldata.jdbc.JdbcConnection} this is an SQL SELECT statement)
 * - <code>update</code> describes how to write data (for {@link ilog.opl.externaldata.jdbc.JdbcConnection} this is an SQL INSERT or UPDATE statement)
 */
public class DataBaseDataHandler extends CustomOplDataHandler {
	/** Info about connections.
	 * This is the data we get from <code>PREFIXConnection</code> statements in the .dat.
	 * We don't open connections immediately. Instead we store away the information and
	 * use it only if we actually ever read/write with the connection.
	 */
	public static class ConnectionInfo {
		/** The identifier used in the <code>PREFIXConnection</code> statement. */
		public final String name;
		/** The connection string (first argument to <code>PREFIXConnection</code>). */
		public final String connstr;
		/** Extra SQL commands (second argument to <code>PREFIXConnection</code>). */
		public final String extra;
		public ConnectionInfo(String name, String connstr, String extra) {
			super();
			this.name = name;
			this.connstr = connstr;
			this.extra = extra;
		}
	}
	/** Connection specifications obtained from <code>PREFIXConnection</code> statements. */
	private final Map<String, ConnectionInfo> specs = new TreeMap<String, ConnectionInfo>();
	
	/** An open connection to a database. */
	private static class DbConnection extends ConnectionInfo {
		public final DataConnection conn;
		public DbConnection(ConnectionInfo info, DataConnection conn) {
			super(info.name, info.connstr, info.extra);
			this.conn = conn;
		}
	}
	/** Connections that were opened for reading. */
	private final Map<String, DbConnection> readConnections = new TreeMap<String, DbConnection>();
	
	/** Info about an element to be published. */
	private static final class PublishInfo extends ConnectionInfo {
		/** Name of the element to be published. */
		public final String elem;
		/** SQL statement to use when publishing <code>elem</code>. */
		public final String spec;
		public PublishInfo(ConnectionInfo info, String elem, String spec) {
			super(info.name, info.connstr, info.extra);
			this.elem = elem;
			this.spec = spec;
		}
	}
	/** The list of elements that should be published.
	 * Elements are in the order in which the <code>PREFIXPublish</code> statements appear in the .dat
	 */
	private final LinkedList<PublishInfo> publish = new LinkedList<PublishInfo>();
	
	/** Look up a database connection and create it if it does not exist yet.
	 * @param name    Name of the connection.
	 * @param factory The factory that is used to create new connections
	 * @param write   If <code>true</code> and the connection must be created then the extra SQL commands for it are executed.
	 * @param conns   The list of connection specifications (used in case the connection has to be created).
	 * @param map     The map of already open connections.
	 * @return The connection for <code>name</code>.
	 * @throws IloException If no connection <code>name</code> was defined.
	 */
	private static DbConnection getOrMakeConnection(String name, ConnectionFactory factory, boolean write, Map<String, ConnectionInfo> conns, Map<String, DbConnection> map) throws IloException, IOException {
		try {
			DbConnection conn = map.get(name);
			if (conn != null)
				return conn;
			final ConnectionInfo info = conns.get(name);
			if (info == null)
				throw new IloException("no connection " + name);
			conn = new DbConnection(info, factory.newConnection(info, write));
			map.put(name, conn);
			return conn;
		}
		catch (Throwable e) {
			// We catch and rethrow any problem. In addition, we print error message and
			// stack trace. This is supposed to make debugging a lot more easy.
			System.err.println(e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}
	
	/** Custom data publisher that writes data to the database. */
	private static final class Publisher extends CustomOplResultPublisher {
		private final String prefix;
		/** Shared with the enclosing {@link DataBaseDataHandler} class. */
		private final Map<String, ConnectionInfo> connectionSpecs;
		/** Shared with the enclosing {@link DataBaseDataHandler} class. */
		private final LinkedList<PublishInfo> publishList;
		/** Shared with the enclosing {@link DataBaseDataHandler} class. */
		private final ConnectionFactory factory;
		private final IloOplModel model;
		
		public Publisher(String prefix, ConnectionFactory factory, Map<String, ConnectionInfo> connectionSpecs, LinkedList<PublishInfo> publishList, IloOplModel model) {
			super(IloOplFactory.getOplFactoryFrom(model));
			this.prefix = prefix;
			this.factory = factory;
			this.connectionSpecs = connectionSpecs;
			this.publishList = publishList;
			this.model = model;
		}

		/** This function is invoked by OPL when data should be published. */
		@Override
		public void customPublish() throws IloException {
			final Map<String, DbConnection> connectionMap = new TreeMap<String, DbConnection>();
			try {
				try {
					while (!publishList.isEmpty()) {
						final PublishInfo info = publishList.removeFirst();
						final IloOplElement elem = model.getElement(info.elem);
						if (elem == null)
							throw new IloException("no element " + info.elem);					
						try {
							System.out.println("Writing " + info.elem + " as " + info.spec);
							final DbConnection conn = getOrMakeConnection(info.name, factory, true, connectionSpecs, connectionMap);
							OutputRowIterator output = conn.conn.openOutputRows(info.spec);
							try {
								DataExporter.exportElement(model, elem, output);
								output.close();
							}
							catch (IOException e) {
								reportAndMap(e);
							}
							finally {
								try { output.close(); }
								catch (IOException e) {
									System.err.println(e.getMessage());
									e.printStackTrace();
									// ignored
								}
							}
						}
						catch (IOException e) {
							reportAndMap(e);
						}
					}
					clearConnectionMap(connectionMap);
				}
				finally {
					// Even in case of error we remove all statements so that a potential
					// next round starts with a clean slate.
					publishList.clear();
				}
			}
			catch (Exception e) {
				reportAndMap(e);
			}
			finally {
				// Even in case of error attempt to close all connections but ignore exceptions.
				// Note that if there is no error then connectionMap is already empty at this
				// point and the function call is a noop.
				try {
					clearConnectionMap(connectionMap);
				}
				catch (IloException ignored) {
					System.err.println(ignored.getMessage());
					ignored.printStackTrace();
				}
			}
		}

		@Override
		public String getCustomResultPublisherName() { return prefix; }
	}
	
	/** Report an {@link IloException} on <code>stderr</code> and rethrow it.
	 * @param e The exception to be reported.
	 * @throws IloException <code>e</code>.
	 */
	private static void reportAndThrow(IloException e) throws IloException {
		System.err.println(e.getMessage());
		e.printStackTrace();
		throw e;
	}
	
	/** Report an exception on <code>stderr</code> and rethrow as an instance of {@link IloException}.
	 * @param e The exception to be reported and mapped.
	 * @throws IloException with the same message as <code>e</code>.
	 */
	private static void reportAndMap(Exception e) throws IloException {
		System.err.println(e.getMessage());
		e.printStackTrace();
		throw new IloException(e.getMessage());
	}

	/** Factory method to be implemented by concrete connections. */
	public interface ConnectionFactory {
		public DataConnection newConnection(ConnectionInfo info, boolean write) throws IOException;
	}

	private final ConnectionFactory factory;
	private final IloOplModel opl;
	private final String prefix;
	private final Publisher publisher;
	/** Create a new data handler.
	 * Also registers the newly created handler with <code>model</code> using <code>prefix</code>
	 * as the handler's prefix.
	 * @param prefix
	 * @param model
	 */
	public DataBaseDataHandler(String prefix, IloOplModel model, ConnectionFactory factory) {
		super(IloOplFactory.getOplFactoryFrom(model));
		this.factory = factory;
		this.opl = model;
		this.prefix = prefix;
		this.publisher = new Publisher(prefix, factory, specs, publish, model);
		opl.registerCustomDataHandler(prefix, this);
		opl.addResultPublisher(publisher);
	}
	
	private static void clearConnectionMap(Map<String, DbConnection> map) throws IloException {
		try {
			Exception ex = null;
			for (DbConnection conn : map.values()) {
				try { conn.conn.close(); }
				catch (Exception e) {
					if (ex == null)
						ex = e;
				}
			}
			if (ex != null) {
				reportAndMap(ex);
			}
		}
		catch (IloException e) {
			reportAndThrow(e);
		}
		catch (RuntimeException e) {
			reportAndMap(e);
		}
		finally {
			// Clear the map even in case of error.
			map.clear();
		}
	}
	
	@Override
	public void closeConnections() throws IloException {
		clearConnectionMap(readConnections);
	}
	@Override
	public void handleConnection(String name, String connstr, String extra) throws IloException {
		// Only record the connection information. We will open the connection only when we
		// need it.
		try {
			if (specs.containsKey(name))
				throw new IloException("duplicate connection name " + name);
			specs.put(name, new ConnectionInfo(name, connstr, extra));
		}
		catch (IloException e) {
			reportAndThrow(e);
		}
		catch (RuntimeException e) {
			reportAndMap(e);
		}
	}
	@Override
	public boolean handleInvoke(String name, String funcname) throws IloException {
		throw new IloException("invoke() not supported");
	}
	@Override
	public void handlePublishElement(String connId, String name, String spec) throws IloException {
		// This function is invoked while parsing the .dat file when the parser hits a <code>PREFIXPublish</code> statement.
		// At that point in time the model is not yet solved, so we don't have any data to publish (yet).
		// We therefore record the specification of things to be published and only publish them later
		// from the custom result publisher that we registered in the constructor.
		try {
			final ConnectionInfo info = specs.get(connId);
			if (info == null)
				throw new IloException("unknown connection " + connId);
			publish.add(new PublishInfo(info, name, spec));
		}
		catch (IloException e) {
			reportAndThrow(e);
		}
		catch (RuntimeException e) {
			reportAndMap(e);
		}
	}
	
	
	@Override
	public void handleReadElement(String connId, String name, String spec) throws IloException {
           System.out.println("Read " + name + " from " + connId + " as \"" + spec + "\"");
		try {
			IloOplElementDefinition elem = opl.getModelDefinition().getElementDefinition(name);
			if (elem == null)
				throw new IloException("unknown element " + elem);

			// We first have to execute the statement because we may need the result set's meta data
			// to setup a tuple reader.
			try {
				DbConnection conn = getOrMakeConnection(connId, factory, false, specs, readConnections);
				InputRowIterator input = conn.conn.openInputRows(spec);
				try {
					DataImporter.readElement(elem, getDataHandler(), input);
				}
				catch (IOException e) {
					reportAndMap(e);
				}
				finally {
					try {
						input.close();
					}
					catch (IOException e) {
						System.err.println(e.getMessage());
						e.printStackTrace();
						// ignored
					}
				}
			}
			catch (IOException e) {
				reportAndMap(e);
			}
			catch (RuntimeException e) {
				reportAndMap(e);
			}
		}
		catch (Exception e) {
			reportAndMap(e);
		}
	}
}
