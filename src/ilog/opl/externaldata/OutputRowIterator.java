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

import java.io.IOException;

/** Table-oriented view on output data.
 * Provides a row-wise view on the storage medium for data.
 */
public interface OutputRowIterator {
	/** Set an <code>int</code> value in the current row.
	 * @param index 0-based field index.
	 * @param value Value to set.
	 * @throws IOException if the value cannot be set.
	 */
	public void setInt(int index, int value) throws IOException;
	/** Set a <code>double</code> value in the current row.
	 * @param index 0-based field index.
	 * @param value Value to set.
	 * @throws IOException if the value cannot be set.
	 */
	public void setDouble(int index, double value) throws IOException;
	/** Set a string value in the current row.
	 * @param index 0-based field index.
	 * @param value Value to set.
	 * @throws IOException if the value cannot be set.
	 */
	public void setString(int index, String value) throws IOException;
	/** Complete the current row.
	 * Indicates that no more data will be added to the current row.
	 * This may result in committing data to the underlying storage.
	 * @throws IOException if anything goes wrong.
	 */
	public void completeRow() throws IOException;
	/** Commit all changes made so far.
	 * This also advances the iterator to the next row.
	 * @throws IOException if anything goes wrong.
	 */
	public void commit() throws IOException;
	/** Close this iterator. */
	public void close() throws IOException;
}
