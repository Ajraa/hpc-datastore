package cz.it4i.fiji.datastore;

import client.base.GraphQLException;
import io.restassured.config.RedirectConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static io.restassured.RestAssured.with;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class DatastoreTestBase {
    final protected long TIMEOUT = 10000L;
    protected String uuid;

    protected abstract void initUUID() throws IOException, GraphQLException;

    @Test
    public void createDataset() {
        assertNotNull(uuid, "Dataset was not created");
    }

    public abstract void writeReadOneBlock() throws IOException, GraphQLException;
    public abstract void writeReadTwoBlocks() throws IOException, GraphQLException;
    public abstract void mixedLatest() throws IOException, GraphQLException;
    public abstract void setGetMetadata() throws IOException, GraphQLException;
    public abstract void readNonExistingBlock() throws IOException, GraphQLException;
    public abstract void readE_NE_E_Block() throws IOException, GraphQLException;
    public abstract void addChannels() throws IOException, GraphQLException;

    protected byte[] constructOneBlock(int dim) {
        return constructBlocks(1, dim);
    }

    protected byte[] constructBlocks(int num, int dim) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(dim);
        int sizeOfOneBlock = (dim * dim * dim + 3) * 4;
        byte[] data = new byte[sizeOfOneBlock * num];
        new Random().nextBytes(data);
        for (int i = 0; i < num; i++) {
            int offset = sizeOfOneBlock * i;
            bb.flip();
            bb.get(data, offset + 0, 4);
            bb.clear();
            bb.get(data, offset + 4, 4);
            bb.clear();
            bb.get(data, offset + 8, 4);
        }
        return data;
    }
}
