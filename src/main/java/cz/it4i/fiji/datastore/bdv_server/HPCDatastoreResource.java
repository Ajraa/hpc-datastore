package cz.it4i.fiji.datastore.bdv_server;

import static cz.it4i.fiji.datastore.bdv_server.HPCDatastoreHelper.getThumbnailProvider;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.UUID;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.VERSION_PARAM;

import bdv.spimdata.SpimDataMinimal;
import bdv.spimdata.XmlIoSpimDataMinimal;
import cz.it4i.fiji.datastore.ApplicationConfiguration;
import cz.it4i.fiji.datastore.core.HPCDatastoreImageLoader;
import mpicbg.spim.data.SpimDataException;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;

import javax.inject.Inject;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@GraphQLApi
public class HPCDatastoreResource {
    @Inject
    JsonDatasetListHandlerTS jsonDatasetListHandlerTS;

    @Inject
    GetSpimDataMinimalTS getSpimDataMinimalTS;

    @Inject
    ApplicationConfiguration configuration;

    private Map<String, ThumbnailProviderTS> thumbnailsGenerators =
            new HashMap<>();

    @Query("JSONListDatastoreLoader")
    public String getJSONListDatastoreLoader(@Name(UUID) String uuid, @Context UriInfo uriInfo)
            throws IOException
    {
        return jsonDatasetListHandlerTS.run(uuid, uriInfo.getRequestUri(), true);
    }

    @Query("MetadataXML")
    public String getMetadataXML(@Name(UUID) String uuidStr,
           @Name(VERSION_PARAM) String versionStr, @Context UriInfo uriInfo)
    {
        final XmlIoSpimDataMinimal io = new XmlIoSpimDataMinimal();

        try (final StringWriter ow = new StringWriter()) {
            SpimDataMinimal spimData = getSpimDataMinimalTS.run(uuidStr, versionStr);
            BuildRemoteDatasetXmlTS.run(io, spimData, new HPCDatastoreImageLoader(uriInfo.getRequestUri().toString()), ow);
            return ow.toString();
        }
        catch (IOException | SpimDataException exc) {
            throw new InternalServerErrorException(exc);
        }
    }

    @Query("Thumbnail")
    @Description("Return a thumbnail in base64 encoding")
    public String getThumbnail(@Name(UUID) String uuid,
           @Name(VERSION_PARAM) String version, @Context UriInfo uriInfo) throws IOException {
        ThumbnailProviderTS ts = getThumbnailProvider(uuid, version, thumbnailsGenerators, getSpimDataMinimalTS);
        return ts.runForThumbnail();
    }
}
