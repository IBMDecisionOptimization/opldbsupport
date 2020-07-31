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

import ilog.opl.IloOplDataHandler;
import ilog.opl.IloOplElementDefinition;

import java.io.IOException;

/** OPL element reader.
 */
interface Reader {
	/** Fill <code>elem</code> from <code>rs</code>.
	 * @param elem     The element to be filled.
	 * @param handler  The factory class to actually fill <code>elem</code>.
	 * @param rs       The data with which to fill <code>elem</code>.
	 * @throws IOException if there is any sort of problem.
	 */
	public void read(IloOplElementDefinition elem, IloOplDataHandler handler, InputRowIterator rs) throws IOException;
}
