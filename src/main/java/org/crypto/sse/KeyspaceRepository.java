package org.crypto.sse;

import com.datastax.driver.core.Session;

/**
 * Created by mjq on 2020/8/14.
 */
public class KeyspaceRepository {
    private Session session;

    public KeyspaceRepository(Session session) {
        this.session = session;
    }

    //创造键空间
    public void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                .append(keyspaceName).append(" WITH replication = {")
                .append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(replicationFactor)
                .append("};");

        String query = sb.toString();
        session.execute(query);
    }
//还不知道这有什么用呢
    public void useKeyspace(String keyspace) {
        session.execute("USE " + keyspace);
    }

    /**
     * Method used to delete the specified schema.
     * It results in the immediate, irreversable removal of the keyspace, including all tables and data contained in the keyspace.
     *
     * @param keyspaceName the name of the keyspace to delete.
     */
    public void deleteKeyspace(String keyspaceName) {
        StringBuilder sb = new StringBuilder("DROP KEYSPACE ").append(keyspaceName);

        final String query = sb.toString();

        session.execute(query);
    }
}