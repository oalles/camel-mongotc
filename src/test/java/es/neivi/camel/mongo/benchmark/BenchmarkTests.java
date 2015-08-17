package es.neivi.camel.mongo.benchmark;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Route;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.CamelSpringDelegatingTestContextLoader;
import org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner;
import org.apache.camel.test.spring.MockEndpoints;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.StopWatch;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;

@RunWith(CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { BenchmarkConfiguration.class }, loader = CamelSpringDelegatingTestContextLoader.class)
// @ContextConfiguration(loader = AnnotationConfigContextLoader.class)
@MockEndpoints
// @DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BenchmarkTests {

	// dependency: camel-spring-javaconfig

	private static Logger LOG = LoggerFactory.getLogger(BenchmarkTests.class);

	@Autowired
	private CamelContext camelContext;

	@Autowired
	private MongoClient mongo;

	@EndpointInject(uri = "mock:test")
	private MockEndpoint mock;

	private MongoDatabase db;
	private MongoCollection<Document> eventsCollection;

	private StopWatch stopWatch = new StopWatch(getClass().getSimpleName());

	private ExecutorService executorService;

	@Before
	public void beforeTest() throws Exception {

		eventsCollection = null;
		eventsCollection = getEventsCollection();

		// Drop tracker collection
		getMongoDatabase().getCollection("tracker").drop();

		mock.reset();
	}

	@After
	public void afterTest() {
		for (Route route : camelContext.getRoutes()) {
			try {
				camelContext.stopRoute(route.getId());
			} catch (Exception e) {
				LOG.error("Exception trying to stop de routes\n{}",
						e.toString());
			}
		}
	}

	private void publishEvents(int documentsPerProducer) {
		List<Document> documents = new ArrayList();
		for (int i = 1; i <= documentsPerProducer; i++) {

			Document document = new Document("value", i).append("date",
					new Date());
			documents.add(document);
		}
		db.getCollection(BenchmarkConfiguration.collectionName)
				.withWriteConcern(WriteConcern.JOURNAL_SAFE)
				.insertMany(documents);
	}

	private void oneProducerConsumeFromRoute(final String routeId)
			throws Exception {

		// Insert events
		publishEvents(BenchmarkConfiguration.DOCUMENTS_PER_PRODUCER);

		mock.expectedMessageCount(BenchmarkConfiguration.DOCUMENTS_PER_PRODUCER);
		mock.whenExchangeReceived(1, new Processor() {

			@Override
			public void process(Exchange exchange) throws Exception {
				LOG.debug("\n\nFirst Exchange processed\n\n");
				stopWatch.start(routeId);
			}
		});
		mock.whenExchangeReceived(
				BenchmarkConfiguration.DOCUMENTS_PER_PRODUCER - 1,
				new Processor() {

					@Override
					public void process(Exchange exchange) throws Exception {
						stopWatch.stop();
						LOG.info("\n\nTime Consumed:\n"
								+ stopWatch.prettyPrint() + "\n\n");
					}
				});

		camelContext.startRoute(routeId);

		// Let the route run
		Thread.sleep(3000);
		mock.assertIsSatisfied();
	}

	@Test
	public void apublishToDBDisabledTest() throws Exception {
		oneProducerConsumeFromRoute(BenchmarkConfiguration.DB_PERS_DISABLED);
	}

	@Test
	public void apublishToDBEnabledTest() throws Exception {
		oneProducerConsumeFromRoute(BenchmarkConfiguration.DB_PERS_ENABLED);
	}

	@Test
	public void apublishToESBDisabledTest() throws Exception {
		oneProducerConsumeFromRoute(BenchmarkConfiguration.ESB_PERS_DISABLED);
	}

	@Test
	public void apublishToESBEnabledTest() throws Exception {
		oneProducerConsumeFromRoute(BenchmarkConfiguration.ESB_PERS_ENABLED);
	}

	private void nProducersConsumeFrom(final String routeId) throws Exception {

		// Load EVENTS
		for (int index = 0; index < BenchmarkConfiguration.PRODUCERS; index++) {

			getExecutorService().execute(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(200);
						publishEvents(BenchmarkConfiguration.DOCUMENTS_PER_PRODUCER);
					} catch (InterruptedException e) {
					}
				}
			});
		}

		// Set EXPECTATIONS
		int messagesCount = BenchmarkConfiguration.DOCUMENTS_PER_PRODUCER
				* BenchmarkConfiguration.PRODUCERS;
		mock.expectedMessageCount(messagesCount);
		mock.whenExchangeReceived(1, new Processor() {

			@Override
			public void process(Exchange exchange) throws Exception {
				LOG.debug("\nFirst Exchange processed");
				stopWatch.start(routeId);
			}
		});
		mock.whenExchangeReceived(messagesCount - 1, new Processor() {

			@Override
			public void process(Exchange exchange) throws Exception {
				stopWatch.stop();
				LOG.info("\n" + stopWatch.prettyPrint() + "\n");
			}
		});

		camelContext.startRoute(routeId);

		getExecutorService().awaitTermination(120, TimeUnit.SECONDS);
		// Thread.sleep(30000);
		mock.assertIsSatisfied();
	}

	@Test
	public void nPublishToDBDisabledTest() throws Exception {
		nProducersConsumeFrom(BenchmarkConfiguration.DB_PERS_DISABLED);
	}

	@Test
	public void nPublishToDBEnabledTest() throws Exception {
		nProducersConsumeFrom(BenchmarkConfiguration.DB_PERS_ENABLED);
	}

	@Test
	public void nPublishToESBDisabledTest() throws Exception {
		nProducersConsumeFrom(BenchmarkConfiguration.ESB_PERS_DISABLED);
	}

	@Test
	public void nPublishToESBEnabledTest() throws Exception {
		nProducersConsumeFrom(BenchmarkConfiguration.ESB_PERS_ENABLED);
	}

	public MongoDatabase getMongoDatabase() {
		if (db == null) {
			db = mongo.getDatabase(BenchmarkConfiguration.dbName);
		}
		return db;
	}

	public MongoCollection<Document> getEventsCollection() {
		if (eventsCollection == null) {
			MongoDatabase db = getMongoDatabase();
			eventsCollection = db
					.getCollection(BenchmarkConfiguration.collectionName);
			eventsCollection.drop();

			// Create collection
			getMongoDatabase()
					.createCollection(
							BenchmarkConfiguration.collectionName,
							new CreateCollectionOptions()
									.capped(true)
									.sizeInBytes(100000000)
									.maxDocuments(
											BenchmarkConfiguration.DOCUMENTS_PER_PRODUCER
													* BenchmarkConfiguration.PRODUCERS)); //

			eventsCollection = db
					.getCollection(BenchmarkConfiguration.collectionName);
		}
		return eventsCollection;
	}

	public ExecutorService getExecutorService() {
		if (executorService == null) {
			executorService = camelContext.getExecutorServiceManager()
					.newFixedThreadPool(this, "MONGO ROUTE", 1);
		}
		return executorService;
	}
}