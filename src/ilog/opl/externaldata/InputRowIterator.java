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
package ilog.opl.externaldata;

import ilog.opl.IloOplTupleSchemaDefinition;

import java.io.IOException;

public interface InputRowIterator {
	/** Get the <code>index</code>-th field in the current row as <code>int</code>.
	 * @param index  0-based column index.
	 * @return The queried data.
	 * @throws IOException If the index is invalid or the data item cannot be read or is not an integer.
	 */
	public int getInt(int index) throws IOException;
	/** Get the <code>index</code>-th field in the current row as <code>double</code>.
	 * @param index   0-based column index.
	 * @return The queried data.
	 * @throws IOException If the index is invalid or the data item cannot be read or is not a double.
	 */
	public double getDouble(int index) throws IOException;
	/** Get the <code>index</code>-th field in the current row as <code>string</code>.
	 * @param index   0-based column index.
	 * @return The queried data.
	 * @throws IOException if the index is invalid or the data item cannot be read or is not an string.
	 */
	public String getString(int index) throws IOException;
	/** Moves to the next input row.
	 * If there are more rows then the iterator moves to the next available row
	 * and returns <code>true</code>. Otherwise it does nothing and returns
	 * <code>false</code>. It is undefined behavior to call {@link #getDouble(int)},
	 * {@link #getInt(int)}, or {@link #getString(int)} without having called this
	 * method at least once or after this method returned <code>false</code>.
	 * *
	 * Initially, an iterator is <b>before</b> the first row.
	 * @return <code>true</code> if more data is available, <code>false</code> otherwise.
	 * @throws IOException if an error occurs.
	 */
	public boolean next() throws IOException;
	
	/** Get the number of columns in the current row.
	 * @return The number of columns in the current row.
	 * @throws IOException if there is no current row or the column count cannot be determined.
	 */
	public int getColumnCount() throws IOException;
	
	/** Release all resources allocated for this iterator. */
	public void close() throws IOException;

	/** Create a tuple descriptor for this iterator.
	 * This is iterator specific since tuples may be loaded by field names in an arbitrary
	 * order of fields. Hence the iterator may have to normalize the indexing into the
	 * tuple. In order to create a default tuple descriptor call {@link TupleIO#TupleIO(IloOplTupleSchemaDefinition)}.
	 * @param schema The schema for the tuple to load.
	 * @return A tuple descriptor for the given schema.
	 * @throws IOException if the schema cannot be constructed.
	 */
	public TupleIO makeTupleIO(IloOplTupleSchemaDefinition schema) throws IOException;
}
