package cz.it4i.fiji.datastore.bdv_server;

import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.UUID;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.VERSION_PARAM;

import com.google.common.base.Strings;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;

import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@GraphQLApi
public class BigDataServerResource {
    private static final String P_PARAM = "p";
    @Inject
    JsonDatasetListHandlerTS jsonDatasetListHandlerTS;

    @Inject
    CellHandlerGQLProducer cellHandlerTSProducer;

    @Context
    UriInfo uri;

    @Query
    public DataReturn getJSONList(@Name(UUID) String uuid) throws IOException {
        return new DataReturn(
                DataReturn.ReturnType.JSON,
                jsonDatasetListHandlerTS.run(uuid, uri.getRequestUri(), false)
        );
    }

    @Query
    public DataReturn getCell(@Name(UUID) String uuid,
                          @Name(VERSION_PARAM) String version,
                          @Name(P_PARAM) String cellString) {
        CellHandlerGQL ts = cellHandlerTSProducer.produce(uri.getBaseUri(), uuid, version);

        if (Strings.emptyToNull(cellString) != null) {
            return ts.runForDataset();
        }

        return ts.runForCellOrInit(cellString);
    }

    @Query
    public DataReturn getSettings(@Name(UUID) String uuid,
            @Name(VERSION_PARAM) String version)
    {
        CellHandlerGQL ts = cellHandlerTSProducer.produce(uri.getBaseUri(), uuid, version);
        return ts.runForSettings();
    }

    @Query
    public DataReturn getThumbnail(@Name(UUID) String uuid,
                                   @Name(VERSION_PARAM) String version) throws IOException {
        CellHandlerGQL ts = cellHandlerTSProducer.produce(uri.getBaseUri(), uuid, version);
        return ts.runForThumbnail();
    }
}
