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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import ilog.concert.IloException;
import ilog.concert.IloSymbolSet;
import ilog.concert.IloTuple;
import ilog.concert.IloTupleSchema;
import ilog.opl.IloCustomOplDataSource;
import ilog.opl.IloOplElementDefinition;
import ilog.opl.IloOplElementDefinitionType;
import ilog.opl.IloOplFactory;
import ilog.opl.IloOplModel;
import ilog.opl.externaldata.DataConnection;
import ilog.opl.externaldata.DataImporter;
import ilog.opl.externaldata.InputRowIterator;
import ilog.opl.externaldata.jdbc.JdbcConnection;

/** A custom data source that reads data from a datasource using JDBC.
 * For this the database input must be configured using a tuple like this:
 * <pre>
 * tuple T {
 *   string connstr;
 *   {string} data;
 *   {string} extra;
 * }
 * {T} customLoad = ...;
 * </pre>
 * where the <code>customLoad</code> element must be set in the .dat file. In this tuple
 * the fields have the following meaning:
 * - <code>connstr</code> the JDBC connection string.
 * - <code>data</code> is a list of string of the form ELEM=SQL where ELEM is the name of
 *   the model element to be filled and SQL is the command to fill with.
 * - <code>extra</code> is a list of SQL statements that are executed right after creation
 *   of the connection.
 *
 * Note that this is standalone and not related to {@link DataBaseDataHandler}.
 */
public class DataBaseDataSource extends IloCustomOplDataSource {
	public static final String ELEMENT_NAME = "customLoad";
	public static final String FIELD_CONNSTR = "connstr";
	public static final String FIELD_DATA = "data";
	public static final String FIELD_EXTRA = "extra";
	private final IloOplModel model;
	public DataBaseDataSource(IloOplModel model) {
		super(IloOplFactory.getOplFactoryFrom(model));
		this.model = model;
	}
	
	/** Get the index of field <code>name</code> in <code>tuple</code>.
	 * If the field does not exist and <code>mustExit</code> is <code>true</code> then
	 * an {@ IllegalArgumentException} is thrown.
	 * @param tuple      The tuple to be queried.
	 * @param name       The name of the field to find.
	 * @param mustExist  If <code>true</code> then non-existing fields trigger an exception.
	 * @return           The index of <code>name</code> or -1 if no such field in <code>tuple</code>.
	 */
	private int getTupleField(IloTuple tuple, String name, boolean mustExist) {
		final IloTupleSchema schema = tuple.getSchema();
		for (int i = 0; i < schema.getSize(); ++i) {
			if (schema.getColumnName(i).equals(name))
				return i;
		}
		if (mustExist)
			throw new IllegalArgumentException("field " + name + " does not exist in " + ELEMENT_NAME);
		return -1;
	}
	
	/** Read the data specified in <code>tuple</code>.
	 * @param tuple The tuple with the data specification. See the comment at the of this class for
	 *              details about the tuple format.
	 */
	private void read(IloTuple tuple) {
		try {
			final int extraIdx = getTupleField(tuple, FIELD_EXTRA, false);
			final IloSymbolSet extra = extraIdx >= 0 ? tuple.getSymbolSetValue(extraIdx) : null;

			// Create connection and execute extra commands.
			final String connstr = tuple.getStringValue(getTupleField(tuple, FIELD_CONNSTR, true));
			DataConnection conn = null;
			if (connstr.startsWith("jdbc:")) {
				Connection jdbc = DriverManager.getConnection(connstr);
				try {
					if (extra != null) {
						for (Iterator<?> it = extra.iterator(); it.hasNext(); /* nothing */) {
							final Statement stmt = jdbc.createStatement();
							try {
								stmt.execute(it.next().toString());
							}
							finally {
								stmt.close();
							}
						}
					}
					conn = new JdbcConnection(jdbc);
					jdbc = null;
				}
				finally {
					if (jdbc != null)
						jdbc.close();
				}
			}
			else {
				throw new IllegalArgumentException("cannot handle connection string " + connstr);
			}

			// Load the data.
			try {
				final int dataIdx = getTupleField(tuple, FIELD_DATA, true);
				for (Iterator<?> it = tuple.getSymbolSetValue(dataIdx).iterator(); it.hasNext(); /* nothing */) {
					final String[] data = it.next().toString().split("=", 2);
					if (data.length != 2)
						throw new IllegalArgumentException("invalid argument " + data[0]);
					final String name = data[0];
					final String spec = data[1];
					final InputRowIterator input = conn.openInputRows(spec);
					try {
						DataImporter.readElement(model.getModelDefinition().getElementDefinition(name), getDataHandler(), input);
					}
					finally {
						input.close();
					}
				}
			}
			finally {
				conn.close();
			}
		}
		catch (IloException e) {
			throw new RuntimeException(e);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void customRead() {
		final IloOplElementDefinition def = model.getModelDefinition().getElementDefinition(ELEMENT_NAME);
		if (def == null)
			return;
		if (def.getElementDefinitionType().equals(IloOplElementDefinitionType.Type.TUPLE)) {
			read(model.getElement(ELEMENT_NAME).asTuple());
		}
		else {
			System.err.println("WARNING: Ignoring element " + ELEMENT_NAME + " because it is not a tuple");
			return;
		}
	}
}
