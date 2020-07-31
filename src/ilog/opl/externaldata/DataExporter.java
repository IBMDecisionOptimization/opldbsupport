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
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import ilog.concert.IloDiscreteDataCollection;
import ilog.concert.IloException;
import ilog.concert.IloIntMap;
import ilog.concert.IloMapIndexArray;
import ilog.concert.IloNumMap;
import ilog.concert.IloSymbolMap;
import ilog.concert.IloTuple;
import ilog.concert.IloTupleMap;
import ilog.concert.IloTupleSet;
import ilog.opl.IloOplElement;
import ilog.opl.IloOplElementType;
import ilog.opl.IloOplFactory;
import ilog.opl.IloOplModel;

/** Class to write out OPL elements.
 * The implementation is based on the various interfaces in this package that provide
 * an abstract, row-oriented view on the storage medium. 
 */
public class DataExporter {
	
	/** Interface for generic writing of data to databases. */
	private interface Writer {
		public void write(Object data, OutputRowIterator output) throws IOException;
	}

	/** Writer for plain <code>int</code> values. */
	private static final Writer INT_WRITER = new Writer() {
		@Override
		public void write(Object data, OutputRowIterator output) throws IOException {
			output.setInt(0, (Integer)data);
		}
	};
	/** Writer for plain <code>double</code> values. */
	private static final Writer NUM_WRITER = new Writer() {
		@Override
		public void write(Object data, OutputRowIterator output) throws IOException {
			output.setDouble(0, (Double)data);
		}
	};
	/** Writer for plain string values. */
	private static final Writer STRING_WRITER = new Writer() {
		@Override
		public void write(Object data, OutputRowIterator output) throws IOException {
			output.setString(0, (String)data);
		}
	};
	/** Writer for tuple values. */
	private static class TupleWriter implements Writer {
		/** Stack for context while writing nested tuples. */
		private static final class Stack {
			public final int tupleIdx;
			public final IloTuple tuple;
			public Stack(int tupleIdx, IloTuple tuple) {
				super();
				this.tupleIdx = tupleIdx;
				this.tuple = tuple;
			}
		}
		private final Vector<Stack> stack = new Vector<Stack>();
		private final TupleIO.TupleSpec[] fields;
		
		public TupleWriter(IloTuple def) {
			final Vector<TupleIO.TupleSpec> v = new Vector<TupleIO.TupleSpec>();
			TupleIO.makeTupleSpec(def, v, "");
			fields = v.toArray(new TupleIO.TupleSpec[v.size()]);
		}
		
		@Override
		public void write(Object data, OutputRowIterator output) throws IOException {
			stack.clear();
			IloTuple t = (IloTuple)data;
			int paramIdx = 0;
			int tupleIdx = 0;
			for (TupleIO.TupleSpec field : fields) {
				switch (field.action) {
				case INT: output.setInt(paramIdx++, t.getIntValue(tupleIdx++)); break;
				case NUM: output.setDouble(paramIdx++, t.getNumValue(tupleIdx++)); break;
				case STR: output.setString(paramIdx++, t.getStringValue(tupleIdx++)); break;
				case START:
					stack.add(new Stack(tupleIdx, t));
					t = t.makeTupleValue(tupleIdx);
					tupleIdx = 0;
					break;
				case END:
					Stack s = stack.remove(stack.size() - 1);
					t = s.tuple;
					tupleIdx = s.tupleIdx + 1;
					break;
				}
			}
		}
	}
	
	/** The different index types that we support for collections. */
	private static enum IndexType {
		INT, NUM, STRING, TUPLE;
		public static IndexType findType(IloDiscreteDataCollection idx) throws IloException {
			if (idx.isIntCollectionColumn() || idx.isIntDataColumn() || idx.isIntRange() || idx.isIntSet())
				return INT;
			else if (idx.isNumCollectionColumn() || idx.isNumDataColumn() || idx.isNumRange() || idx.isNumSet())
				return NUM;
			else if (idx.isSymbolSet())
				return STRING;
			else if (idx.isTupleSet())
				return TUPLE;
			else
				throw new IloException("unsupported index type");
		}
	}
	
	private static abstract class IndexIterator<E> implements Iterator<E> {
		private final Iterator<?> idx;
		public IndexIterator(Iterator<?> idx) { this.idx = idx; }
		@Override
		public boolean hasNext() { return idx.hasNext(); }
		@Override
		public E next() {
			try {
				return map(idx.next());
			}
			catch (IloException e) {
				throw new RuntimeException(e.getMessage());
			}
		}
		protected abstract E map(Object key) throws IloException;
		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}			
	}
	
	private static final class IntMapIterator extends IndexIterator<Object> {
		private final IndexType indexType;
		private final IloIntMap data;
		public IntMapIterator(IloIntMap data, IloDiscreteDataCollection idx) throws IloException {
			super(idx.iterator());
			this.indexType = IndexType.findType(idx);
			this.data = data;
		}
		protected Object map(Object key) throws IloException {
			switch (indexType) {
			case INT: return data.get((Integer)key);
			case NUM: return data.get((Double)key);
			case STRING: return data.get((String)key);
			case TUPLE: return data.get((IloTuple)key);
			}
			return null; // not reached
		}
	}
	
	private static final class NumMapIterator extends IndexIterator<Object> {
		private final IndexType indexType;
		private final IloNumMap data;
		public NumMapIterator(IloNumMap data, IloDiscreteDataCollection idx) throws IloException {
			super(idx.iterator());
			this.indexType = IndexType.findType(idx);
			this.data = data;
		}
		protected Object map(Object key) throws IloException {
			switch (indexType) {
			case INT: return data.get((Integer)key);
			case NUM: return data.get((Double)key);
			case STRING: return data.get((String)key);
			case TUPLE: return data.get((IloTuple)key);
			}
			return null; // not reached
		}
	}
	
	private static final class StringMapIterator extends IndexIterator<Object> {
		private final IndexType indexType;
		private final IloSymbolMap data;
		public StringMapIterator(IloSymbolMap data, IloDiscreteDataCollection idx) throws IloException {
			super(idx.iterator());
			this.indexType = IndexType.findType(idx);
			this.data = data;
		}
		protected Object map(Object key) throws IloException {
			switch (indexType) {
			case INT: return data.get((Integer)key);
			case NUM: return data.get((Double)key);
			case STRING: return data.get((String)key);
			case TUPLE: return data.get((IloTuple)key);
			}
			return null; // not reached
		}
	}
	
	private static final class TupleMapIterator extends IndexIterator<Object> {
		private final IndexType indexType;
		private final IloTupleMap data;
		private final IloMapIndexArray array;
		private final IloTuple buffer;
		public TupleMapIterator(IloTupleMap data, IloDiscreteDataCollection idx, IloMapIndexArray array, IloTuple buffer) throws IloException {
			super(idx.iterator());
			this.indexType = IndexType.findType(idx);
			this.data = data;
			this.array = array;
			this.buffer = buffer;
		}
		protected Object map(Object key) throws IloException {
			array.clear();
			switch (indexType) {
			case INT: array.add((Integer)key); break;
			case NUM: array.add((Double)key); break;
			case STRING: array.add((String)key); break;
			case TUPLE: array.add((IloTuple)key); break;
			}
			data.getAt(array, buffer);
			return buffer;
		}
	}

	
	public static void exportElement(IloOplModel model, IloOplElement elem, OutputRowIterator output) throws IOException, IloException {
		Writer writer = null;
		Iterator<?> data = null;
		if (elem.getElementType().equals(IloOplElementType.Type.INT)) {
			writer = INT_WRITER;
			data = Collections.singleton(elem.asInt()).iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.NUM)) {
			writer = NUM_WRITER;
			data = Collections.singleton(elem.asNum()).iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.STRING)) {
			writer = STRING_WRITER;
			data = Collections.singleton(elem.asString()).iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.TUPLE)) {
			writer = new TupleWriter(elem.asTuple());
			data = Collections.singleton(elem.asTuple()).iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.SET_INT)) {
			writer = INT_WRITER;
			data = elem.asIntSet().iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.SET_NUM)) {
			writer = NUM_WRITER;
			data = elem.asNumSet().iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.SET_SYMBOL)) {
			writer = STRING_WRITER;
			data = elem.asSymbolSet().iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.SET_TUPLE)) {
			final IloTupleSet ts = elem.asTupleSet();
			if (ts.getSize() > 0)
				writer = new TupleWriter(ts.makeFirst());
			else
				writer = null;
			data = elem.asTupleSet().iterator();
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.MAP_INT)) {
			final IloIntMap map = elem.asIntMap();
			final int dim = map.getNbDim();
			switch (dim) {
			case 1:
				writer = INT_WRITER;
				data = new IntMapIterator(map, map.makeMapIndexer().get(0));
				break;
			default: throw new IloException("cannot output element " + elem.getName() + " of dimension " + dim);
			}
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.MAP_NUM)) {
			final IloNumMap map = elem.asNumMap();
			final int dim = map.getNbDim();
			switch (dim) {
			case 1:
				writer = NUM_WRITER;
				data = new NumMapIterator(map, map.makeMapIndexer().get(0));
				break;
			default: throw new IloException("cannot output element " + elem.getName() + " of dimension " + dim);
			}
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.MAP_SYMBOL)) {
			final IloSymbolMap map = elem.asSymbolMap();
			final int dim = map.getNbDim();
			switch (dim) {
			case 1:
				writer = STRING_WRITER;
				data = new StringMapIterator(map, map.makeMapIndexer().get(0));
				break;
			default: throw new IloException("cannot output element " + elem.getName() + " of dimension " + dim);
			}
		}
		else if (elem.getElementType().equals(IloOplElementType.Type.MAP_TUPLE)) {
			final IloTupleMap map = elem.asTupleMap();
			final int dim = map.getNbDim();
			switch (dim) {
			case 1:
				final IloDiscreteDataCollection idx = map.makeMapIndexer().get(0);
				final IloTuple buffer = map.makeTuple();
				if (idx.getSize() > 0) {  
					writer = new TupleWriter(buffer);
				}
				else {
					// empty array
					writer = null;
				}
				data = new TupleMapIterator(map, idx, IloOplFactory.getOplFactoryFrom(model).mapIndexArray(1), buffer);
				break;
			default: throw new IloException("cannot output element " + elem.getName() + " of dimension " + dim);
			}
		}
		else
			throw new IloException("cannot output element " + elem.getName() + " of type " + elem.getElementType());
		
		// Now publish the element.
		while (data.hasNext()) {
			writer.write(data.next(), output);
			output.completeRow();
		}
		output.commit();
	}
}
