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
import ilog.opl.IloOplDataHandler;
import ilog.opl.IloOplArrayDefinition;
import ilog.opl.IloOplElementDefinition;
import ilog.opl.IloOplElementDefinitionType;

import java.io.IOException;

/** Class to read OPL elements.
 * The implementation is based on the interfaces in this package that provide a
 * row-oriented view on the input medium.
 */
public class DataImporter {
	/** Read a single scalar.
	 * Base class for reading scalar values.
	 */
	private static abstract class ValueReader implements Assign, Reader {
		@Override
		public void read(IloOplElementDefinition elem, IloOplDataHandler handler, InputRowIterator input) throws IOException {
			handler.restartElement(elem.getName());
			if (!input.next())
				throw new IOException("no data for element " + elem.getName());
			assign(handler, input);
			handler.endElement();
		}
		public abstract void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException;
	}

	/** Read a flat <code>int</code>. */
	private static final ValueReader INT_READER = new ValueReader() {

		@Override
		public void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException {
			handler.addIntItem(input.getInt(0));
		}

	};
	/** Read a flat <code>double</code>. */
	private static final ValueReader NUM_READER = new ValueReader() {

		@Override
		public void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException {
			handler.addNumItem(input.getDouble(0));
		}

	};
	/** Read a flat string. */
	private static final ValueReader STRING_READER = new ValueReader() {

		@Override
		public void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException {
			handler.addStringItem(input.getString(0));
		}

	};

	/** Helpers for input of collections. */
	private static abstract class CollectionHelper {
		public abstract void start(IloOplDataHandler handler);
		public abstract void end(IloOplDataHandler handler);

		public static final CollectionHelper SET = new CollectionHelper() {
			@Override
			public void start(IloOplDataHandler handler) { handler.startSet(); }
			@Override
			public void end(IloOplDataHandler handler) { handler.endSet(); }
		};
		public static final CollectionHelper ARRAY = new CollectionHelper() {
			@Override
			public void start(IloOplDataHandler handler) { handler.startArray(); }
			@Override
			public void end(IloOplDataHandler handler) { handler.endArray(); }
		};

	}

	/** Read a collection where each row in the result set defines one element in the collection. */
	private static final class CollectionReader implements Reader {
		private final Assign assign;
		private final CollectionHelper helper;
		public CollectionReader(Assign assign, CollectionHelper helper) {
			super();
			this.assign = assign;
			this.helper = helper;
		}
		@Override
		public final void read(IloOplElementDefinition elem, IloOplDataHandler handler, InputRowIterator input) throws IOException {
			handler.restartElement(elem.getName());
			helper.start(handler);
			while (input.next())
				assign.assign(handler, input);
			helper.end(handler);
			handler.endElement();
		}
	}
	private static final Reader INTSET_READER = new CollectionReader(INT_READER, CollectionHelper.SET);
	private static final Reader NUMSET_READER = new CollectionReader(NUM_READER, CollectionHelper.SET);
	private static final Reader STRINGSET_READER = new CollectionReader(STRING_READER, CollectionHelper.SET);
	private static final Reader INTARRAY_READER = new CollectionReader(INT_READER, CollectionHelper.ARRAY);
	private static final Reader NUMARRAY_READER = new CollectionReader(NUM_READER, CollectionHelper.ARRAY);
	private static final Reader STRINGARRAY_READER = new CollectionReader(STRING_READER, CollectionHelper.ARRAY);

	/** Fill a collection from a row.
	 * This is used to fill two-dimensional collections.
	 */
	private static abstract class RowAssign implements Assign {
		private final CollectionHelper helper;
		protected abstract void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException;
		protected RowAssign(CollectionHelper helper) { this.helper = helper; }
		@Override
		public final void assign(IloOplDataHandler handler, InputRowIterator input) throws IOException {
			helper.start(handler);
			final int cols = input.getColumnCount();
			for (int i = 0; i < cols; ++i)
				doAssign(handler, input, i);
			helper.end(handler);
		}		
	}
	private static final Assign INTROWSET_READER = new RowAssign(CollectionHelper.SET) {
		@Override
		protected void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException { handler.addIntItem(input.getInt(col)); }
	};
	private static final Assign NUMROWSET_READER = new RowAssign(CollectionHelper.SET) {
		@Override
		protected void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException { handler.addNumItem(input.getDouble(col)); }
	};
	private static final Assign STRINGROWSET_READER = new RowAssign(CollectionHelper.SET) {
		@Override
		protected void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException { handler.addStringItem(input.getString(col)); }
	};
	private static final Assign INTROWARRAY_READER = new RowAssign(CollectionHelper.ARRAY) {
		@Override
		protected void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException { handler.addIntItem(input.getInt(col)); }
	};
	private static final Assign NUMROWARRAY_READER = new RowAssign(CollectionHelper.ARRAY) {
		@Override
		protected void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException { handler.addNumItem(input.getDouble(col)); }
	};
	private static final Assign STRINGROWARRAY_READER = new RowAssign(CollectionHelper.ARRAY) {
		@Override
		protected void doAssign(IloOplDataHandler handler, InputRowIterator input, int col) throws IOException { handler.addStringItem(input.getString(col)); }
	};

	/** Fill <code>elem</code> from <code>input</code>.
	 * @param elem     The element to be filled.
	 * @param handler  The factory that stores actual data into <code>elem</code>.
	 * @param input    Row-oriented view on input data.
	 * @throws IOException if there is a problem.
	 * @throws IloException if there is a problem.
	 */
	public static void readElement(IloOplElementDefinition elem, IloOplDataHandler handler, InputRowIterator input) throws IOException, IloException {
		Reader reader = null;
		final IloOplElementDefinitionType.Type type = elem.getElementDefinitionType();
		if (type.equals(IloOplElementDefinitionType.Type.INTEGER))
			reader = INT_READER;
		else if (type.equals(IloOplElementDefinitionType.Type.FLOAT))
			reader = NUM_READER;
		else if (type.equals(IloOplElementDefinitionType.Type.STRING))
			reader = STRING_READER;
		else if (type.equals(IloOplElementDefinitionType.Type.TUPLE))
			reader = input.makeTupleIO(elem.asTuple().getTupleSchema());
		else if (type.equals(IloOplElementDefinitionType.Type.SET)) {
			final IloOplElementDefinition item = elem.asSet().getItem();
			final IloOplElementDefinitionType.Type itemType = item.getElementDefinitionType();
			if (itemType.equals(IloOplElementDefinitionType.Type.INTEGER))
				reader = INTSET_READER;
			else if (itemType.equals(IloOplElementDefinitionType.Type.FLOAT))
				reader = NUMSET_READER;
			else if (itemType.equals(IloOplElementDefinitionType.Type.STRING))
				reader = STRINGSET_READER;
			else if (itemType.equals(IloOplElementDefinitionType.Type.TUPLE))
				reader = new CollectionReader(input.makeTupleIO(item.asTuple().getTupleSchema()), CollectionHelper.SET);
			else if (itemType.equals(IloOplElementDefinitionType.Type.ARRAY)) {
				// Set of arrays
				final IloOplElementDefinitionType.Type subType = item.asArray().getItem().getElementDefinitionType();
				if (subType.equals(IloOplElementDefinitionType.Type.INTEGER))
					reader = new CollectionReader(INTROWARRAY_READER, CollectionHelper.SET);
				else if (subType.equals(IloOplElementDefinitionType.Type.FLOAT))
					reader = new CollectionReader(NUMROWARRAY_READER, CollectionHelper.SET);
				else if (subType.equals(IloOplElementDefinitionType.Type.STRING))
					reader = new CollectionReader(STRINGROWARRAY_READER, CollectionHelper.SET);
				else
					throw new IloException("cannot read set of arrays " + elem.getName() + " with sub type " + subType);
			}
			else if (itemType.equals(IloOplElementDefinitionType.Type.SET)) {
				// Set of sets
				final IloOplElementDefinitionType.Type subType = item.asArray().getItem().getElementDefinitionType();
				if (subType.equals(IloOplElementDefinitionType.Type.INTEGER))
					reader = new CollectionReader(INTROWSET_READER, CollectionHelper.SET);
				else if (subType.equals(IloOplElementDefinitionType.Type.FLOAT))
					reader = new CollectionReader(NUMROWSET_READER, CollectionHelper.SET);
				else if (subType.equals(IloOplElementDefinitionType.Type.STRING))
					reader = new CollectionReader(STRINGROWSET_READER, CollectionHelper.SET);
				else
					throw new IloException("cannot read set of sets " + elem.getName() + " with sub type " + subType);
			}
			else
				throw new IloException("cannot read set " + elem.getName() + " of type " + itemType);
		}
		else if (type.equals(IloOplElementDefinitionType.Type.ARRAY)) {
			final IloOplArrayDefinition array = elem.asArray();
			final int dims = array.getDimensions();
			final IloOplElementDefinition item = array.getItem();
			final IloOplElementDefinitionType.Type itemType = item.getElementDefinitionType();
			if ( dims == 1 ) {
				if (itemType.equals(IloOplElementDefinitionType.Type.INTEGER))
					reader = INTARRAY_READER;
				else if (itemType.equals(IloOplElementDefinitionType.Type.FLOAT))
					reader = NUMARRAY_READER;
				else if (itemType.equals(IloOplElementDefinitionType.Type.STRING))
					reader = STRINGARRAY_READER;
				else if (itemType.equals(IloOplElementDefinitionType.Type.TUPLE))
					reader = new CollectionReader(input.makeTupleIO(item.asTuple().getTupleSchema()), CollectionHelper.ARRAY);
				else if (itemType.equals(IloOplElementDefinitionType.Type.SET)) {
					// Array of sets
					final IloOplElementDefinitionType.Type subType = item.asArray().getItem().getElementDefinitionType();
					if (subType.equals(IloOplElementDefinitionType.Type.INTEGER))
						reader = new CollectionReader(INTROWSET_READER, CollectionHelper.ARRAY);
					else if (subType.equals(IloOplElementDefinitionType.Type.FLOAT))
						reader = new CollectionReader(NUMROWSET_READER, CollectionHelper.ARRAY);
					else if (subType.equals(IloOplElementDefinitionType.Type.STRING))
						reader = new CollectionReader(STRINGROWSET_READER, CollectionHelper.ARRAY);
					else
						throw new IloException("cannot read array of sets " + elem.getName() + " with sub type " + subType);
				}
				else
					throw new IloException("Cannot read " + dims + "D array of " + itemType + " for " + elem.getName());
			}
			else if ( dims == 2 ) {
				// Array of arrays
				if (itemType.equals(IloOplElementDefinitionType.Type.INTEGER))
					reader = new CollectionReader(INTROWARRAY_READER, CollectionHelper.ARRAY);
				else if (itemType.equals(IloOplElementDefinitionType.Type.FLOAT))
					reader = new CollectionReader(NUMROWARRAY_READER, CollectionHelper.ARRAY);
				else if (itemType.equals(IloOplElementDefinitionType.Type.STRING))
					reader = new CollectionReader(STRINGROWARRAY_READER, CollectionHelper.ARRAY);
				else
					throw new IloException("Cannot read " + dims + "D array of " + itemType + " for " + elem.getName());
			}
		}
		else {
			throw new IloException("cannot read element " + elem.getName() + " of type " + elem.getElementDefinitionType() + " from database");
		}

		// Now read the result set into the OPL element.
		reader.read(elem, handler, input);
	}
}
