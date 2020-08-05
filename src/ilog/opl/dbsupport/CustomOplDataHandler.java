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

import ilog.concert.IloException;
import ilog.concert.cppimpl.IloEnv;
import ilog.opl.IloOplCustomDataHandler;
import ilog.opl.IloOplCustomDataHandlerWrapper;
import ilog.opl.IloOplDataHandler;
import ilog.opl.IloOplFactory;

/** Base class for custom data handlers.
 * Extend this class to implement a data handler that can be used with
 * {@link ilog.opl.IloOplModel#registerCustomDataHandler(String, ilog.opl.IloOplCustomDataHandler)}.
 * See the C++ reference documentation for details about the abstract functions
 * that must be implemented by subclasses.
 * Note that this class should eventually become part of the official OPL API in which case it can
 * be removed from this project.
 */
public abstract class CustomOplDataHandler extends IloOplCustomDataHandler {
	private static final class Wrapper extends IloOplCustomDataHandlerWrapper {
		private CustomOplDataHandler wrapped;
		public Wrapper(IloEnv env) { super(env); }
		public void setWrapped(CustomOplDataHandler wrapped) { this.wrapped = wrapped; }
		@Override
		public void closeConnections() {
			try { wrapped.closeConnections(); }
			catch (IloException e) { throw new RuntimeException(e); }
		}
		@Override
		public void handleConnection(String name, String connection, String extra) {
			try { wrapped.handleConnection(name, connection, extra); }
			catch (IloException e) { throw new RuntimeException(e); }
		}
		@Override
		public boolean handleInvoke(String name, String funcname) {
			try { return wrapped.handleInvoke(name, funcname); }
			catch (IloException e) { throw new RuntimeException(e); }
		}
		@Override
		public void handlePublishElement(String connId, String name, String spec) {
			try { wrapped.handlePublishElement(connId, name, spec); }
			catch (IloException e) { throw new RuntimeException(e); }
		}
		@Override
		public void handleReadElement(String connId, String name, String spec) {
			try { wrapped.handleReadElement(connId, name, spec); }
			catch (IloException e) { throw new RuntimeException(e); }
		}
	}
	
	private final Wrapper wrapper;
	
	private CustomOplDataHandler(Wrapper wrapper) {
		super(wrapper);
		this.wrapper = wrapper;
		wrapper.setWrapped(this);
	}
	
	public CustomOplDataHandler(IloOplFactory factory) {
		this(new Wrapper(factory.getEnv()));
	}
	
	protected IloOplDataHandler getDataHandler() { return wrapper.getDataHandler(); }
	
	public abstract void closeConnections() throws IloException;
	public abstract void handleConnection(String name, String connection, String extra) throws IloException;
	public abstract boolean handleInvoke(String name, String funcname) throws IloException;
	public abstract void handlePublishElement(String connId, String name, String spec) throws IloException;
	public abstract void handleReadElement(String connId, String name, String spec) throws IloException;
}
