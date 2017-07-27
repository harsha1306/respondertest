/*
 * Copyright © 2014-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package cdap.responder.test;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import org.apache.tephra.TransactionFailureException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * This is a simple ResponderTests example that uses one stream, one dataset, one flow and one service.
 * <uL>
 * <li>A stream to send names to.</li>
 * <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 * <li>A service that reads the name from the KeyValueTable and responds with 'Hello [Name]!'</li>
 * </uL>
 */
public class ResponderTests extends AbstractApplication {

	@Override
	public void configure() {
		setName("ResponderTests");
		setDescription("A CDAP module to run various tests on the responder");
		createDataset("whom", KeyValueTable.class, DatasetProperties.builder().setDescription("Store names").build());
		addService(new ResponderTest());
	}

	/**
	 * A Service that creates a greeting using a user's name.
	 */
	public static final class ResponderTest extends AbstractService {

		public static final String SERVICE_NAME = "ResponderTest";

		@Override
		protected void configure() {
			setName(SERVICE_NAME);
			setDescription("Service to test responder");
			addHandler(new ResponderTestHandler());
		}
	}

	/**
	 * ResponderTest Service handler.
	 */
	public static final class ResponderTestHandler extends AbstractHttpServiceHandler {

		@UseDataSet("whom")
		private KeyValueTable whom;

		private Metrics metrics;

		@Path("readkey/{key}")
		@GET
		public void greet(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("key") String key) {
			byte[] value = whom.read(key);
			if (value != null)
				responder.sendString(String.format("Value is %s!", new String(value)));
			else
				responder.sendStatus(404);
		}

		@Path("add/predelay/{key}/{value}")
		@POST
		public void addPreDelay(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("key") String key, @PathParam("value") String value) {
      		whom.write(key,value);

      		responder.sendStatus(201);

			delay();
		}

		@Path("add/postdelay/{key}/{value}")
		@POST
		public void addPostDelay(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("key") String key, @PathParam("value") String value) {
			whom.write(key,value);

			delay();

			responder.sendStatus(201);
		}

		@Path("add/transaction/{key}/{value}")
		@POST
		@TransactionPolicy(TransactionControl.EXPLICIT)
		public void addTransaction(HttpServiceRequest request, HttpServiceResponder responder, final @PathParam("key") String key, final @PathParam("value") String value) {
			try {
				getContext().execute(new TxRunnable() {
					@Override
					public void run(DatasetContext context) throws Exception {
						whom.write(key,value);
					}
				});
			} catch (TransactionFailureException e) {
				e.printStackTrace();
			}

			responder.sendStatus(201);

			delay();
		}

		void delay(){
			for(int i =0;i<100;i++)
				for(int k =0;k<10000000;k++){
					int j = 0;
					j++;
					String s = j+" ";
					s.trim();
				}
		}
	}
}
