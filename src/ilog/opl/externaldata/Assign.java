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

import java.io.IOException;

/** Interface to fill an OPL element with data.
 */
interface Assign {
	/** Fill the current element with data.
	 * @param handler The factory class through which the current element is filled.
	 * @param input   Iterable on the input data.
	 * @throws IOException if there was any sort of problem.
	 */
	public void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException;
}
