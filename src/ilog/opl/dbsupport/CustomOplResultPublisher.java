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
import ilog.opl.IloOplFactory;
import ilog.opl.IloOplResultPublisher;
import ilog.opl.IloOplResultPublisherWrapper;

/** Base class for custom result publishers.
 * Extend this class to implement a data handler that can be used with
 * {@link ilog.opl.IloOplModel#addResultPublisher(ilog.opl.IloOplResultPublisher)}.
 * See the C++ reference documentation for details about the abstract functions
 * that must be implemented by subclasses.
 * Note that this class should eventually become part of the official OPL API in which case it can
 * be removed from this project.
 */
public abstract class CustomOplResultPublisher extends IloOplResultPublisher {
	// Internal class to wrap the actual implementation of a result publisher.
	private static final class Wrapper extends IloOplResultPublisherWrapper {
		private CustomOplResultPublisher wrapped;
		public Wrapper(IloEnv env) { super(env); }
		
		public void setWrapped(CustomOplResultPublisher wrapped) { this.wrapped = wrapped; }

		@Override
		public String getResultPublisherName() { return wrapped.getCustomResultPublisherName(); }

		@Override
		public void publish() {
			try { wrapped.customPublish(); }
			catch (IloException e) { throw new RuntimeException(e.getMessage()); }
		}		
	}
	
	// Private constructor for syntactical sugar.
	// What we would like to have is a constructor
	//   IloOplCustomResultPublisher(IloOplFactory factory) {
	//      Wrapper w = new Wrapper(factory.getEnv());
	//      super(w);
	//      w.setWrapped(this)
	//   }
	// But that is illegal in Java. Hence we work around with this private constructor.
	private CustomOplResultPublisher(Wrapper wrapper) {
		super(wrapper);
		wrapper.setWrapped(this);
	}
	
	public CustomOplResultPublisher(IloOplFactory factory) { this(new Wrapper(factory.getEnv())); }

	@Override
	public final String getResultPublisherName() { return getCustomResultPublisherName(); }

	public abstract void customPublish() throws IloException;
	public abstract String getCustomResultPublisherName();
}
