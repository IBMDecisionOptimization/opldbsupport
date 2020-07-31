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

/** Interface to data connection.
 * This interface is used by the {@link DataImporter} and {@link DataExporter} class to abstract
 * out the actual data reading and writing.
 * It provides a simple, table-oriented view on data.
 */
public interface DataConnection {
	/** Construct an input data instance from <code>command</code>. */
	public InputRowIterator openInputRows(String command) throws IOException;
	/** Construct an output data instance from <code>command</code>. */
	public OutputRowIterator openOutputRows(String command) throws IOException;
	/** Close this connection. */
	public void close() throws IOException;
}
