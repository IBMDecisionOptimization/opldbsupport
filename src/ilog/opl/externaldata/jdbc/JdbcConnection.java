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
package ilog.opl.externaldata.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import ilog.concert.IloException;
import ilog.opl.IloOplModel;
import ilog.opl.IloOplTupleSchemaDefinition;
import ilog.opl.dbsupport.DataBaseDataHandler;
import ilog.opl.dbsupport.DataBaseDataHandler.ConnectionInfo;
import ilog.opl.externaldata.DataConnection;
import ilog.opl.externaldata.InputRowIterator;
import ilog.opl.externaldata.OutputRowIterator;
import ilog.opl.externaldata.TupleIO;

public class JdbcConnection implements DataConnection {
	
	private static boolean traceEnabled = false;
	
	public static boolean setTraceEnabled(boolean set) {
		final boolean old = traceEnabled;
		traceEnabled = set;
		return old;
	}
	
	public static void trace(String s) {
		if (traceEnabled)
			System.err.print(s);
	}
	public static void traceln(String s) {
		if (traceEnabled)
			System.err.println(s);
	}
	
	/** Wrap an arbitrary exception into an {@link IOException} and rethrow it.
	 * If tracing is enabled then it also prints a description of <code>e</code>.
	 * @param e The exception to be wrapped.
	 * @throws IOException that wraps <code>e</code>.
	 */
	private static void wrapException(Exception e) throws IOException {
		if (traceEnabled) {
			traceln(e.getMessage());
			e.printStackTrace(System.err);
		}
		throw new IOException(e);
	}
	
	public static class InputStatement implements InputRowIterator {
		private Statement stmt;
		private ResultSet rs;
		private boolean ok = true;
		private int columns = -1;
		public InputStatement(Connection conn, String stmt) throws SQLException {
			Statement s = conn.createStatement();
			try {
				rs = s.executeQuery(stmt);
				this.stmt = s;
				s = null;
				traceln("JdbcConnection.InputStatememt(" + stmt + "): " + rs.getMetaData().getColumnCount() + " columns");
			}
			finally {
				if (s != null)
					s.close();
			}
		}
		@Override
		public int getInt(int index) throws IOException {
			try {
				return rs.getInt(index + 1);
			}
			catch (SQLException e) {
				wrapException(e);
				return 0; // not reached
			}
		}
		@Override
		public double getDouble(int index) throws IOException {
			try {
				return rs.getDouble(index + 1);
			}
			catch (SQLException e) {
				wrapException(e);
				return 0; // not reached
			}
		}
		@Override
		public String getString(int index) throws IOException {
			try {
				return rs.getString(index + 1);
			}
			catch (SQLException e) {
				wrapException(e);
				return null; // not reached
			}
		}
		@Override
		public boolean next() throws IOException {
			if (!ok)
				return false;
			try {
				ok = rs.next();
				return ok;
			}
			catch (SQLException e) {
				wrapException(e);
				return false; // not reached
			}
		}
		@Override
		public int getColumnCount() throws IOException {
			try {
				if (columns < 0)
					columns = rs.getMetaData().getColumnCount();
			}
			catch (SQLException e) {
				wrapException(e);
			}
			return columns;
		}
		@Override
		public void close() throws IOException {
			ok = false;
			final Statement s = stmt;
			stmt = null;
			final ResultSet r = rs;
			rs = null;
			try {
				if (r != null)
					r.close();
				if (s != null)
					s.close();
			}
			catch (SQLException e) {
				wrapException(e);
			}
		}
		@Override
		public TupleIO makeTupleIO(IloOplTupleSchemaDefinition schema) throws IOException {
			try {
				final ResultSetMetaData meta = rs.getMetaData();
				return new TupleIO(schema, new TupleIO.TableMetaData() {
					@Override
					public int getColumnCount() throws IOException {
						try { return meta.getColumnCount(); }
						catch (SQLException e) { throw new IOException(e); }
					}
					@Override
					public String getColumnName(int column) throws IOException {
						try { return meta.getColumnName(column + 1); }
						catch (SQLException e) { throw new IOException(e); }
					}
				});
			}
			catch (SQLException e) {
				wrapException(e);
				return null; // not reached
			}
			catch (IloException e) {
				wrapException(e);
				return null; // not reached
			}
		}
		
	}
	public static class OutputStatement implements OutputRowIterator {
		private PreparedStatement stmt;
		public OutputStatement(Connection conn, String sql) throws SQLException {
			stmt = conn.prepareStatement(sql);
			traceln("JdbcConnection.OutputStatement(" + sql + ")");
		}
		@Override
		public void setInt(int index, int value) throws IOException {
			try {
				stmt.setInt(index + 1, value);
			}
			catch (SQLException e) {
				wrapException(e);
			}
		}
		@Override
		public void setDouble(int index, double value) throws IOException {
			try {
				stmt.setDouble(index + 1, value);
			}
			catch (SQLException e) {
				wrapException(e);
			}
		}
		@Override
		public void setString(int index, String value) throws IOException {
			try {
				stmt.setString(index + 1, value);
			}
			catch (SQLException e) {
				wrapException(e);
			}
		}
		@Override
		public void completeRow() throws IOException {
			try {
				stmt.addBatch();
			}
			catch (SQLException e) {
				wrapException(e);
			}
		}
		@Override
		public void commit() throws IOException {
			try {
				stmt.executeBatch(); /** TODO: Check the return value? */
			}
			catch (SQLException e) {
				wrapException(e);
			}
		}
		@Override
		public void close() throws IOException {
			final PreparedStatement s = stmt;
			stmt = null;
			if (s != null) {
				try {
					s.close();
				}
				catch (SQLException e) {
					wrapException(e);
				}
			}
		}
	}
	
	private Connection conn;
	
	public JdbcConnection(String connstr) throws SQLException {
		conn = DriverManager.getConnection(connstr);
	}
	
	public JdbcConnection(String connstr, String username, String password) throws SQLException {
		conn = DriverManager.getConnection(connstr, username, password);
	}
	
	public JdbcConnection(String connstr, Properties info) throws SQLException {
		conn = DriverManager.getConnection(connstr, info);
	}
	/**
	 * <b>Attention</b>: the newly created instance takes ownership of the passed connection!
	 * @param conn
	 */
	public JdbcConnection(Connection conn) {
		this.conn = conn;
	}
	
	
	@Override
	public InputRowIterator openInputRows(String command) throws IOException {
		traceln("JdbcConnection: openInputRows(" + command + ")");
		try {
			return new InputStatement(conn, command);
		}
		catch (SQLException e) {
			wrapException(e);
			return null; // not reached
		}
	}
	@Override
	public OutputRowIterator openOutputRows(String command) throws IOException {
		traceln("JdbcConnection: openOutputRows(" + command + ")");
		try {
			return new OutputStatement(conn, command);
		}
		catch (SQLException e) {
			wrapException(e);
			return null; // not reached
		}
	}

	@Override
	public void close() throws IOException {
		final Connection c = conn;
		conn = null;
		if (c != null) {
			try { c.close(); }
			catch (SQLException e) { wrapException(e); }
		}
	}
	
	/** Convenience function to create and register data handlers.
	 * This is intended to be called from a .dat file's <code>prepare</code> section
	 * and provides an easy way to add database support to a .dat file.
	 * @param prefix
	 * @param model
	 */
	public static void register(String prefix, IloOplModel model) {
		System.err.println("Registering " + prefix);
		new DataBaseDataHandler(prefix, model, new DataBaseDataHandler.ConnectionFactory() {
			@Override
			public DataConnection newConnection(ConnectionInfo info, boolean write) throws IOException {
				try {
					Connection c = DriverManager.getConnection(info.connstr);
					try {
						if (write) {
							for (String sql : info.extra.split(";")) {
								final String cmd = sql.trim();
								if (cmd.length() > 0) {
									JdbcConnection.traceln("execute >" + cmd + "<");
									final Statement stmt = c.createStatement();
									try {
										stmt.execute(cmd);
									}
									finally {
										stmt.close();
									}
								}
							}
						}
						JdbcConnection jdbc = new JdbcConnection(c);
						c = null;
						return jdbc;
					}
					finally {
						if (c != null)
							c.close();
					}
				}
				catch (SQLException e) {
					throw new IOException(e);
				}
			}
		});
		System.err.println("Prefix " + prefix + " registered for JDBC");
	}
}
