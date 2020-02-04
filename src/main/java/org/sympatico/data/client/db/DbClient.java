package org.sympatico.data.client.db;

public abstract class DbClient {

    public DbClientType dbClientType;

    public DbClient(DbClientType dbClientType) {
        this.dbClientType = dbClientType;
    }

    abstract public void set(String key, byte[] json);

    abstract public byte[] get(String key);

    abstract protected void shutdown();
}
