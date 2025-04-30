package cz.it4i.fiji.datastore;

import client.DataServerManagerService;
import client.RegisterService;
import client.SharedDatasetServerService;
import cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceResource;
import cz.it4i.fiji.datastore.register_service.ResolutionLevel;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.config.RedirectConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import lombok.extern.log4j.Log4j2;
import models.ConnectionParameters;
import models.DataReturn;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import client.base.*;

import static io.restassured.RestAssured.with;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

@Log4j2
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGraphQLDatastore extends DatastoreTestBase {

    RegisterService registerService;

    @BeforeAll
    public void initialize() {
        GraphQLClient client = GraphQLClient.getInstance(getUrl());
        registerService = new RegisterService(client);
    }

    @BeforeEach
    @Override
    public void initUUID() throws IOException, GraphQLException {
        if (uuid != null) return;
        UUID id = registerService.createEmptyDataset(getDatasetDTO());
        uuid = id.toString();
    }

    @Test
    @Override
    public void writeReadOneBlock() throws IOException, GraphQLException {
        GraphQLClient client = GraphQLClient.getInstance(getUrl());
        DataServerManagerService dataServerManager = new DataServerManagerService(client);

        ConnectionParameters params = registerService.startServer(uuid, 1, 1, 1, "new", TIMEOUT, "write");
        byte[] data = constructOneBlock(64);
        String stringData = Base64.getEncoder().encodeToString(data);

        SharedDatasetServerService serverService = new SharedDatasetServerService(client, params);
        DataReturn ret = serverService.writeBlock(0, 0, 0, 0, 0, 0, "", stringData);
        log.info(ret.getReturnType().toString());

        params = registerService.startServer(uuid, 1, 1, 1, "latest", TIMEOUT, "read");

        serverService = new SharedDatasetServerService(client, params);
        DataReturn read = serverService.readBlock(0, 0, 0, 0, 0, 0, "");

        assertEquals(DataReturn.ReturnType.BASE64, read.getReturnType(), "Result was " + read.getReturnType().toString());


        //assertEquals(stringData, read.getData(), "Result was different");
        byte[] outputData = Base64.getDecoder().decode(read.getData());
        assertArrayEquals(data, outputData);
    }

    @Test
    @Override
    public void writeReadTwoBlocks() throws IOException, GraphQLException {
        GraphQLClient client = GraphQLClient.getInstance(getUrl());
        DataServerManagerService dataServerManager = new DataServerManagerService(client);
        ConnectionParameters params = registerService.startServer(uuid, 1, 1, 1, "new", TIMEOUT, "write");

        byte[] data = constructBlocks(2, 64);
        String stringData = Base64.getEncoder().encodeToString(data);

        SharedDatasetServerService serverService = new SharedDatasetServerService(client, params);
        DataReturn ret = serverService.writeBlock(0, 0, 0, 0, 0, 0, "/0/1/0/0/0/0", stringData);

        params = registerService.startServer(uuid, 1, 1, 1, "latest", TIMEOUT, "read");

        serverService = new SharedDatasetServerService(client, params);
        DataReturn read = serverService.readBlock(0, 0, 0, 0, 0, 0, "/0/1/0/0/0/0");
        byte[] outputData = Base64.getDecoder().decode(read.getData());
        assertArrayEquals(data, outputData);
    }

    @Test
    @Override
    public void mixedLatest() throws IOException, GraphQLException {
        byte[] block1 = constructBlocks(1, 64);
        String stringData1 = Base64.getEncoder().encodeToString(block1);

        byte[] block2 = constructBlocks(1, 64);
        String stringData2 = Base64.getEncoder().encodeToString(block2);

        byte[] sentData = new byte[block1.length + block2.length];
        System.arraycopy(block1, 0, sentData, 0, block1.length);
        System.arraycopy(block2, 0, sentData, block1.length, block2.length);

        GraphQLClient client = GraphQLClient.getInstance(getUrl());
        DataServerManagerService dataServerManager = new DataServerManagerService(client);

        ConnectionParameters params = registerService.startServer(uuid, 1, 1, 1, "new", TIMEOUT, "write");
        SharedDatasetServerService serverService = new SharedDatasetServerService(client, params);

        DataReturn ret = serverService.writeBlock(0, 0, 0, 0, 0, 0, "", stringData1);

        params = registerService.startServer(uuid, 1, 1, 1, "new", TIMEOUT, "write");
        serverService = new SharedDatasetServerService(client, params);

        ret = serverService.writeBlock(0, 1, 0, 0, 0, 0, "", stringData2);

        params = registerService.startServer(uuid, 1, 1, 1, "mixedLatest", TIMEOUT, "read");

        serverService = new SharedDatasetServerService(client, params);
        DataReturn read = serverService.readBlock(0, 0, 0, 0, 0, 0, "/0/1/0/0/0/0");

        byte[] outputData = Base64.getDecoder().decode(read.getData());
        assertArrayEquals(sentData, outputData);
    }

    @Test
    @Override
    public void setGetMetadata() throws IOException, GraphQLException {
        String testMetadata = "test metadata" + Math.random();
        registerService.setCommonMetadata(uuid, testMetadata);

        String readMetadata = registerService.getCommonMetadata(uuid).getData();
        assertEquals(testMetadata, readMetadata);
    }

    @Test
    @Override
    public void readNonExistingBlock() throws IOException, GraphQLException {
        GraphQLClient client = GraphQLClient.getInstance(getUrl());
        DataServerManagerService dataServerManager = new DataServerManagerService(client);

        ConnectionParameters params = registerService.startServer(uuid, 1, 1, 1, "latest", TIMEOUT, "read");

        SharedDatasetServerService serverService = new SharedDatasetServerService(client, params);
        DataReturn read = serverService.readBlock(10, 10, 10, 0, 0, 0, "");
        byte[] outputData = Base64.getDecoder().decode(read.getData());


        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.putInt(-1);
        bb.putInt(-1);
        bb.putInt(-1);
        byte[] data = bb.array();
        assertArrayEquals(data, outputData);
    }

    @Test
    @Override
    public void readE_NE_E_Block() throws IOException, GraphQLException {
        GraphQLClient client = GraphQLClient.getInstance(getUrl());

        byte[] data = constructBlocks(2, 64);
        String dataString = Base64.getEncoder().encodeToString(data);

        ConnectionParameters params = registerService.startServer(uuid, 1, 1, 1, "new", TIMEOUT, "write");
        SharedDatasetServerService serverService = new SharedDatasetServerService(client, params);

        DataReturn ret = serverService.writeBlock(0, 0, 0, 0, 0, 0, "/0/1/0/0/0/0", dataString);


        params = registerService.startServer(uuid, 1, 1, 1, "latest", TIMEOUT, "read");

        serverService = new SharedDatasetServerService(client, params);
        DataReturn read = serverService.readBlock(0, 0, 0, 0, 0, 0, "/10/10/10/0/0/0/0/1/0/0/0/0");
        byte[] outputData = Base64.getDecoder().decode(read.getData());

        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.putInt(-1);
        bb.putInt(-1);
        bb.putInt(-1);
        byte[] nonExistingData = bb.array();
        bb = ByteBuffer.allocate(data.length + nonExistingData.length);
        bb.put(data, 0, data.length / 2);
        bb.put(nonExistingData);
        bb.put(data, data.length / 2, data.length / 2);
        data = bb.array();
        assertArrayEquals(data, outputData);
    }

    @Test
    @Override
    public void addChannels() throws IOException, GraphQLException {
        GraphQLClient client = GraphQLClient.getInstance(getUrl());

        for (int i = 0; i < 2; i++) {
            ConnectionParameters params = registerService.startServer(uuid, 1, 1, 1, "new", TIMEOUT, "write");
        }
        registerService.addChannels(uuid, "10");
    }

    private models.QLDatasetDTO getDatasetDTO() {
        models.QLDatasetDTO ret = new models.QLDatasetDTO();

        ret.setVoxelType("uint32");
        List<Long> dimensions = new ArrayList<>();
        dimensions.add(1000L);
        dimensions.add(1000L);
        dimensions.add(1L);

        ret.setDimensions(dimensions);
        ret.setTimepoints(2);
        ret.setChannels(2);
        ret.setAngles(2);
        ret.setVoxelUnit("um");

        List<Float> voxelResolution = new ArrayList<>();
        voxelResolution.add(0.4f);
        voxelResolution.add(0.4f);
        voxelResolution.add(1.0f);
        ret.setVoxelResolution(voxelResolution);

        ret.setTimepointResolution(new models.QLResolution("min", 1));
        ret.setChannelResolution(new models.QLResolution(null, 0));
        ret.setAngleResolution(new models.QLResolution(null, 0));

        ret.setCompression("raw");

        List<models.QLResolutionLevel> resolutionLevels = new ArrayList<>();
        resolutionLevels.add(new models.QLResolutionLevel(Arrays.asList(1, 1, 1), Arrays.asList(64, 64, 64)));
        resolutionLevels.add(new models.QLResolutionLevel(Arrays.asList(2, 2, 1), Arrays.asList(64, 64, 64)));

        ret.setResolutionLevels(resolutionLevels);
        return ret;
    }

    private String getUrl() {
        String host = System.getProperty("test-host", "localhost"); // Default to "localhost" if not set
        String port = System.getProperty("test-port", "9080"); // Default to "9080" if not set
        return new StringBuilder().append("http://").append(host).append(":").append(port).append("/graphql").toString();
    }
}
