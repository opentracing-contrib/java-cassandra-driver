package io.opentracing.contrib.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentracing.contrib.spanmanager.DefaultSpanManager;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CassandraTest {
    private static final MockTracer mockTracer = new MockTracer(MockTracer.Propagator.TEXT_MAP);

    @BeforeClass
    public static void init() {
        GlobalTracer.register(mockTracer);
    }

    @Before
    public void before() throws Exception {
        mockTracer.reset();
        DefaultSpanManager.getInstance().clear();
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    @After
    public void after() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Test
    public void withoutParent() throws Exception {
        Session session = createSession();

        ResultSetFuture future = session.executeAsync("SELECT * FROM system.schema_keyspaces;");

        ResultSet resultSet = future.get();
        assertNotNull(resultSet.one());

        PreparedStatement prepared = session.prepare(
                "create keyspace test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
        BoundStatement bound = prepared.bind();
        session.execute(bound);

        session.execute("USE test;");
        session.execute("SELECT * FROM system.schema_keyspaces;");

        List<MockSpan> finished = mockTracer.finishedSpans();
        assertEquals(4, finished.size());

        for (MockSpan mockSpan : finished) {
            assertEquals(0, mockSpan.parentId());
        }

        checkSpans(finished);
    }

    @Test
    public void withParent() throws Exception {
        Session session = createSession();

        MockSpan parent = mockTracer.buildSpan("parent").start();
        DefaultSpanManager.getInstance().activate(parent);

        session.executeAsync("SELECT * FROM system.schema_keyspaces;").get();
        session.execute("SELECT * FROM system.schema_keyspaces;");

        List<MockSpan> finished = mockTracer.finishedSpans();
        assertEquals(2, finished.size());
        for (MockSpan mockSpan : finished) {
            assertEquals(parent.context().spanId(), mockSpan.parentId());
            assertEquals(parent.context().traceId(), mockSpan.context().traceId());
        }

        checkSpans(finished);
    }

    @Test
    public void usingNewSession() {
        Session session = createNewSession();

        session.execute("SELECT * FROM system.schema_keyspaces;");

        List<MockSpan> finished = mockTracer.finishedSpans();
        assertEquals(1, finished.size());

        checkSpans(finished);
    }

    @Test
    public void connectAsync() throws ExecutionException, InterruptedException {
        ListenableFuture<Session> futureSession = createAsincSession();
        Session session = futureSession.get();

        session.execute("SELECT * FROM system.schema_keyspaces;");

        List<MockSpan> finished = mockTracer.finishedSpans();
        assertEquals(1, finished.size());

        checkSpans(finished);
    }

    @Test
    public void connectAsyncWithKey() throws ExecutionException, InterruptedException {
        ListenableFuture<Session> futureSession = createAsincSessionWithKey();
        Session session = futureSession.get();

        session.execute("SELECT * FROM system.schema_keyspaces;");

        List<MockSpan> finished = mockTracer.finishedSpans();
        assertEquals(1, finished.size());

        checkSpans(finished);
    }

    private void checkSpans(List<MockSpan> mockSpans) {
        for (MockSpan mockSpan : mockSpans) {
            assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
            assertEquals(TracingSession.COMPONENT_NAME, mockSpan.tags().get(Tags.COMPONENT.getKey()));
            assertEquals("cassandra", mockSpan.tags().get(Tags.DB_TYPE.getKey()));
            assertEquals(0, mockSpan.generatedErrors().size());
            String operationName = mockSpan.operationName();
            assertTrue(operationName.equals("execute"));

            assertNotNull(mockSpan.tags().get(Tags.DB_STATEMENT.getKey()));
            assertNotNull(mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));
        }
    }

    private Session createSession() {
        Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
        Cluster cluster = new TracingCluster(builder);
        return cluster.connect();
    }

    private Session createNewSession() {
        Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
        Cluster cluster = new TracingCluster(builder);
        return cluster.newSession();
    }

    private ListenableFuture<Session> createAsincSession() {
        Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
        Cluster cluster = new TracingCluster(builder);
        return cluster.connectAsync();
    }

    private ListenableFuture<Session> createAsincSessionWithKey() {
        Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
        Cluster cluster = new TracingCluster(builder);
        return cluster.connectAsync("system");
    }
}
