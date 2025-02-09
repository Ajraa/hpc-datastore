package cz.it4i.fiji.datastore;

import cz.it4i.fiji.datastore.bdv_server.DataReturn;
import cz.it4i.fiji.datastore.core.Version;
import cz.it4i.fiji.datastore.register_service.OperationMode;
import lombok.extern.log4j.Log4j2;
import mpicbg.spim.data.SpimDataException;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Mutation;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;

import javax.inject.Inject;
import javax.ws.rs.InternalServerErrorException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Collections;

import static cz.it4i.fiji.datastore.DatasetServerEndpoint.*;
import static cz.it4i.fiji.datastore.core.Version.stringToIntVersion;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.*;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.Z_PARAM;
import static cz.it4i.fiji.datastore.register_service.OperationMode.READ;
import static cz.it4i.fiji.datastore.register_service.OperationMode.READ_WRITE;
import static java.util.Collections.singletonList;

@GraphQLApi
@Log4j2
public class SharedDatasetServerResource implements Serializable {

    private static final long serialVersionUID = -7629675047684517279L;

    @Inject
    GQLBlockRequestHandler blockRequestHandler;

    @Inject
    ApplicationConfiguration configuration;

    @Query("SharedStatus")
    public DataReturn getStatus(@Name(UUID) String uuid,
                                @Name(R_X_PARAM) int rX, @Name(R_Y_PARAM) int rY,
                                @Name(R_Z_PARAM) int rZ, @Name(VERSION_PARAM) String version)
    {
        RootResponse result = RootResponse.builder().uuid(uuid).mode(
                        OperationMode.READ_WRITE).version(Version.stringToIntVersion(version))
                .resolutionLevels(Collections.singletonList(
                        new int[] { rX, rY, rZ })).build();
        if (result.getUuid() != null) return new DataReturn(DataReturn.ReturnType.XML, result.getUuid());
        return new DataReturn(DataReturn.ReturnType.HTML, getResponseAsHTML());
    }

    @Query("ReadSharedBlock")
    public DataReturn readBlock(@Name(UUID) String uuid,
                                @Name(R_X_PARAM) int rX, @Name(R_Y_PARAM) int rY,
                                @Name(R_Z_PARAM) int rZ, @Name(VERSION_PARAM) String version,
                                @Name(X_PARAM) long x, @Name(Y_PARAM) long y,
                                @Name(Z_PARAM) long z, @Name(TIME_PARAM) int time,
                                @Name(CHANNEL_PARAM) int channel, @Name(ANGLE_PARAM) int angle,
                                @Name(BLOCKS_PARAM) String blocks) throws IOException {
        return blockRequestHandler.readBlock(getDataSetserver(uuid, rX, rY, rZ, version), x, y, z, time, channel,
                angle, blocks);
    }

    @Mutation("WriteSharedBlock")
    public DataReturn writeBlock(@Name(UUID) String uuid,
                                 @Name(R_X_PARAM) int rX, @Name(R_Y_PARAM) int rY,
                                 @Name(R_Z_PARAM) int rZ, @Name(VERSION_PARAM) String version,
                                 @Name(X_PARAM) long x, @Name(Y_PARAM) long y,
                                 @Name(Z_PARAM) long z, @Name(TIME_PARAM) int time,
                                 @Name(CHANNEL_PARAM) int channel, @Name(ANGLE_PARAM) int angle,
                                 @Name(BLOCKS_PARAM) String blocks,
                                 @Name("inputStream") String inputStream) throws IOException {
        byte[] decodedBytes = Base64.getDecoder().decode(inputStream);
        InputStream stream = new ByteArrayInputStream(decodedBytes);
        return blockRequestHandler.writeBlock(getDataSetserver(uuid, rX, rY, rZ, version), x, y, z, time, channel,
                angle, blocks, stream);
    }

    @Query("SharedType")
    public DataReturn getType(@Name(UUID) String uuid,
                              @Name(R_X_PARAM) int rX, @Name(R_Y_PARAM) int rY,
                              @Name(R_Z_PARAM) int rZ, @Name(VERSION_PARAM) String version,
                              @Name(TIME_PARAM) int time, @Name(CHANNEL_PARAM) int channel,
                              @Name(ANGLE_PARAM) int angle)
    {
        return blockRequestHandler.getType(getDataSetserver(uuid, rX, rY, rZ, version), time, channel, angle);
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

    private DatasetServerImpl getDataSetserver(String uuid, int rX, int rY,
                                               int rZ, String version)
    {
        try {
            final boolean mixedVersion = Version.MIXED_LATEST_VERSION_NAME.equals(
                    version);
            DatasetHandler handler = configuration.getDatasetHandler(uuid);
            int versionInt = mixedVersion?handler.getLatestVersion():stringToIntVersion(version);
            OperationMode mode = mixedVersion ? READ : READ_WRITE;
            return new DatasetServerImpl(handler, singletonList(new int[] { rX, rY,
                    rZ }), versionInt, mixedVersion, mode);
        }
        catch (SpimDataException | IOException exc) {
            log.error("getDatasetServer", exc);
            throw new InternalServerErrorException(exc);
        }
    }
}
