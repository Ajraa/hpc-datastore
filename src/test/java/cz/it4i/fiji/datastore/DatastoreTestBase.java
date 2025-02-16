package cz.it4i.fiji.datastore;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class DatastoreTestBase {
    final protected long TIMEOUT = 10000L;
    protected String uuid;

    protected abstract void initUUID();

    @Test
    public void createDataset() {
        assertNotNull(uuid, "Dataset was not created");
    }

    public abstract void writeReadOneBlock();
    public abstract void writeReadTwoBlocks();
    public abstract void mixedLatest();
    public abstract void setGetMetadata();
    public abstract void readNonExistingBlock();
    public abstract void readE_NE_E_Block();
    public abstract void addChannels();

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
