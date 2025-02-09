package cz.it4i.fiji.datastore;

import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.X_PARAM;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.Y_PARAM;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.Z_PARAM;
import static cz.it4i.fiji.datastore.DatasetServerEndpoint.*;

import cz.it4i.fiji.datastore.bdv_server.DataReturn;
import cz.it4i.fiji.datastore.management.DataServerManager;
import cz.it4i.fiji.datastore.timout_shutdown.TimeoutTimer;
import io.quarkus.security.Authenticated;
import lombok.extern.log4j.Log4j2;
import mpicbg.spim.data.SpimDataException;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Mutation;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Base64;

@Authenticated
@Log4j2
@GraphQLApi
public class DatasetServerResource implements Serializable {

    private static final long serialVersionUID = -3783914337389687663L;
    @Inject
    TimeoutTimer timer;

    @Inject
    DataServerManager dataServerManager;

    @Inject
    GQLBlockRequestHandler blockRequestHandler;

    @Inject
    ApplicationConfiguration configuration;

    private DatasetServerImpl datasetServer;

    @PostConstruct
    void init() {
        try {
            String uuid = dataServerManager.getUUID();
            if (uuid == null || datasetServer != null) {
                return;
            }
            datasetServer = new DatasetServerImpl(configuration.getDatasetHandler(
                    uuid), dataServerManager.getResolutionLevels(), dataServerManager
                    .getVersion(), dataServerManager.isMixedVersion(), dataServerManager
                    .getMode());
            log.info("DatasetServer initialized");
        }
        catch (SpimDataException | IOException exc) {
            log.error("init", exc);
        }
    }

    @Query
    public DataReturn getStatus()
    {
        RootResponse result =  RootResponse.builder().uuid(dataServerManager
                        .getUUID()).mode(
                        dataServerManager.getMode()).version(dataServerManager.getVersion())
                .resolutionLevels(dataServerManager.getResolutionLevels()).serverTimeout(
                        dataServerManager.getServerTimeout()).build();
        if (result.getUuid() != null) return new DataReturn(DataReturn.ReturnType.XML, result.getUuid());
        return new DataReturn(DataReturn.ReturnType.HTML, getResponseAsHTML());
    }

    @Query
    public DataReturn readBlock(@Name(X_PARAM) long x,
                                @Name(Y_PARAM) long y, @Name(Z_PARAM) long z,
                                @Name(TIME_PARAM) int time, @Name(CHANNEL_PARAM) int channel,
                                @Name(ANGLE_PARAM) int angle, @Name(BLOCKS_PARAM) String blocks) throws IOException {
        return blockRequestHandler.readBlock(datasetServer, x, y, z, time, channel,
                angle, blocks);
    }

    @Mutation
    public DataReturn writeBlock(@Name(X_PARAM) long x,
                                 @Name(Y_PARAM) long y, @Name(Z_PARAM) long z,
                                 @Name(TIME_PARAM) int time, @Name(CHANNEL_PARAM) int channel,
                                 @Name(ANGLE_PARAM) int angle, @Name(BLOCKS_PARAM) String blocks,
                                 @Name("inputStream") String inputStream) throws IOException {
        byte[] decodedBytes = Base64.getDecoder().decode(inputStream);
        InputStream stream = new ByteArrayInputStream(decodedBytes);
        return blockRequestHandler.writeBlock(datasetServer, x, y, z, time, channel,
                angle, blocks, stream);
    }

    @Query
    public DataReturn getType(@Name(TIME_PARAM) int time,
                            @Name(CHANNEL_PARAM) int channel, @Name(ANGLE_PARAM) int angle)
    {
        return blockRequestHandler.getType(datasetServer, time, channel, angle);
    }

    private String getResponseAsHTML()
    {
        StringBuilder sb = new StringBuilder();
        String url = "https://github.com/fiji-hpc/hpc-datastore/";
        // @formatter:off
        sb.append(
                        "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en-gb\" lang=\"en-gb\" dir=\"ltr\">").append('\n')
                .append("<head>").append('\n')
                .append("<meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\" />").append('\n')
                .append("</head>").append('\n')
                .append("<body>").append('\n');

        sb
                .append("<h1>HPCDataStore is running.</h1>").append('\n')
                .append("<p>See more on github: <a target=\"_blank\" href=\"" + url +"\">HPCDataStore</a></p>")
                .append("</body>").append('\n');
        // @formatter:on
        return sb.toString();
    }
}
