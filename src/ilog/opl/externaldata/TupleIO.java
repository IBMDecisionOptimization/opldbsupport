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

import ilog.concert.IloException;
import ilog.concert.IloTuple;
import ilog.concert.IloTupleSchema;
import ilog.opl.IloOplDataHandler;
import ilog.opl.IloOplElementDefinition;
import ilog.opl.IloOplElementDefinitionType;
import ilog.opl.IloOplTupleSchemaDefinition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/** Class to simplify input and output of tuples.
 * Instances of this class give a specification of a tuple by a list of <code>TupleSpec</code>s. 
 */
public class TupleIO implements Assign, Reader{
	/** Meta data for a table. */
	public interface TableMetaData {
		/** Get the number of columns. */
		public int getColumnCount() throws IOException;
		/** Get the name for zero-based column index. */
		public String getColumnName(int column) throws IOException;
	}
	/** Descriptor for a tuple field. */
	public static final class TupleSpec {
		/** The action that can be taken for a field. */
		public enum Action {
			/** Fill an <code>int</code> field. */
			INT,
			/** Fill a <code>double</code> field. */
			NUM,
			/** Fill a string field. */
			STR,
			/** Start a sub-tuple */
			START,
			/** End a sub.tuple. */
			END }
		/** Action for this field. */
		public final Action action;
		/** Name of this field. */
		public final String name;
		/** Column index in database to which this field corresponds. */
		public int column;
		public TupleSpec(Action action, String name) {
			this.action = action;
			this.name = name;
			this.column = -1;
		}
	}
	/** Separator used when creating fully qualified names of nested tuple's fields. */
	private static final String SEP = ".";

	/** (Recursively) create a tuple-specification for <code>tuple</code>.
	 * @param tuple       The tuple for which the the specification is to be created.
	 * @param result      The dynamic array that holds all the field specifications for <code>tuple</code>.
	 * @param namePrefix  The name prefix for tuple field names.
	 */
	public static void makeTupleSpec(IloTuple tuple, Vector<TupleSpec> result, String namePrefix) {
		IloTupleSchema schema = tuple.getSchema();
		for (int i = 0; i < schema.getSize(); ++i) {
			if (schema.isInt(i))
				result.add(new TupleSpec(TupleSpec.Action.INT, namePrefix + schema.getColumnName(i)));
			else if (schema.isNum(i))
				result.add(new TupleSpec(TupleSpec.Action.NUM, namePrefix + schema.getColumnName(i)));
			else if (schema.isSymbol(i))
				result.add(new TupleSpec(TupleSpec.Action.STR, namePrefix + schema.getColumnName(i)));
			else if (schema.isTuple(i)) {
				result.add(new TupleSpec(TupleSpec.Action.START, namePrefix + schema.getColumnName(i)));
				makeTupleSpec(tuple.makeTupleValue(i), result, namePrefix + schema.getColumnName(i) + SEP);
				result.add(new TupleSpec(TupleSpec.Action.END, namePrefix + schema.getColumnName(i)));
			}
			else
				throw new UnsupportedOperationException("cannot fill tuple field " + schema.getColumnName(i));
		}
	}

	public static void makeTupleSpec(IloOplTupleSchemaDefinition def, Vector<TupleSpec> result, String namePrefix) {
		for (int i = 0; i < def.getSize(); ++i) {
			IloOplElementDefinition field = def.getComponent(i);
			final IloOplElementDefinitionType.Type fieldType = field.getElementDefinitionType();
			if (fieldType.equals(IloOplElementDefinitionType.Type.INTEGER))
				result.add(new TupleSpec(TupleSpec.Action.INT, namePrefix + field.getName()));
			else if (fieldType.equals(IloOplElementDefinitionType.Type.FLOAT))
				result.add(new TupleSpec(TupleSpec.Action.NUM, namePrefix + field.getName()));
			else if (fieldType.equals(IloOplElementDefinitionType.Type.STRING))
				result.add(new TupleSpec(TupleSpec.Action.STR, namePrefix + field.getName()));
			else if (fieldType.equals(IloOplElementDefinitionType.Type.TUPLE)) {
				result.add(new TupleSpec(TupleSpec.Action.START, namePrefix + field.getName()));
				makeTupleSpec(field.asTuple().getTupleSchema(), result, namePrefix + field.getName() + SEP);
				result.add(new TupleSpec(TupleSpec.Action.END, namePrefix + field.getName()));
			}
			else
				throw new UnsupportedOperationException("cannot fill tuple field of type " + fieldType);
		}
	}
	private static TupleSpec[] makeTupleSpec(IloOplTupleSchemaDefinition def, TableMetaData meta) throws IloException {
		final Vector<TupleSpec> result = new Vector<TupleSpec>();
		makeTupleSpec(def, result, "");
		final TupleSpec[] array = result.toArray(new TupleSpec[result.size()]);

		// Here we implement support for named parameters.
		// If all parameters have a name then we match the parameter names against
		// the tuple field names.
		final Map<String, Integer> name2idx = new HashMap<String, Integer>();
		if (meta != null) {
			try {
				for (int i = 0; i < meta.getColumnCount(); ++i) {
					final int idx = i;
					String col = meta.getColumnName(idx);
					if (col != null && col.length() > 0) {
						if (name2idx.size() != i)
							throw new IloException("either all columns must be named or none");
						name2idx.put(col, idx);
					}
				}
			}
			catch (IOException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				throw new IloException(e.getMessage());
			}
		}
		if (name2idx.size() > 0) {
			for (TupleSpec t : array) {
				if (t.action != TupleSpec.Action.START && t.action != TupleSpec.Action.END) {
					Integer idx = name2idx.get(t.name);
					if (idx == null)
						throw new IloException("no column for field " + t.name);
					t.column = idx;
				}
			}
		}
		else {
			int idx = 0;
			for (TupleSpec t : array) {
				if (t.action != TupleSpec.Action.START && t.action != TupleSpec.Action.END)
					t.column = idx++;
			}
		}

		return array;
	}

	private final TupleSpec[] fields;
	public TupleIO(IloOplTupleSchemaDefinition def) throws IloException {
		this(def, null);
	}
	public TupleIO(IloOplTupleSchemaDefinition def, TableMetaData meta) throws IloException {
		fields = makeTupleSpec(def, meta);
	}

	@Override
	public void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException {
		handler.startTuple();
		for (TupleSpec field : fields) {
			switch (field.action) {
			case INT: handler.addIntItem(input.getInt(field.column)); break;
			case NUM: handler.addNumItem(input.getDouble(field.column)); break;
			case STR: handler.addStringItem(input.getString(field.column)); break;
			case START: handler.startTuple(); break;
			case END: handler.endTuple(); break;
			}
		}
		handler.endTuple();
	}
	@Override
	public void read(IloOplElementDefinition elem, IloOplDataHandler handler, InputRowIterator input) throws IOException {
		handler.restartElement(elem.getName());
		if (!input.next())
			throw new IOException("no data for element " + elem.getName());
		assign(handler, input);
		handler.endElement();
	}
}
