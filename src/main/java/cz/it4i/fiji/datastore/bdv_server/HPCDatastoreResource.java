package cz.it4i.fiji.datastore.bdv_server;

import static cz.it4i.fiji.datastore.bdv_server.HPCDatastoreHelper.getThumbnailProvider;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.UUID;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.VERSION_PARAM;

import bdv.spimdata.SpimDataMinimal;
import bdv.spimdata.XmlIoSpimDataMinimal;
import cz.it4i.fiji.datastore.ApplicationConfiguration;
import cz.it4i.fiji.datastore.core.HPCDatastoreImageLoader;
import mpicbg.spim.data.SpimDataException;
import org.apache.commons.lang.NotImplementedException;
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
import java.net.URI;
import java.net.URISyntaxException;
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
    public DataReturn getJSONListDatastoreLoader(@Name(UUID) String uuid, @Name("uri") String uriString)
            throws IOException, URISyntaxException {
        URI uri = new URI(uriString);
        return new DataReturn(
                DataReturn.ReturnType.JSON,
                jsonDatasetListHandlerTS.run(uuid, uri, true)
        );
    }

    @Query("MetadataXML")
    public DataReturn getMetadataXML(@Name(UUID) String uuidStr,
           @Name(VERSION_PARAM) String versionStr, @Name("uri") String uriString) throws URISyntaxException {
        URI uri = new URI(uriString);
        final XmlIoSpimDataMinimal io = new XmlIoSpimDataMinimal();

        try (final StringWriter ow = new StringWriter()) {
            SpimDataMinimal spimData = getSpimDataMinimalTS.run(uuidStr, versionStr);
            BuildRemoteDatasetXmlTS.run(io, spimData, new HPCDatastoreImageLoader(uri.toString()), ow);
            return new DataReturn(
                    DataReturn.ReturnType.XML,
                    ow.toString()
            );
        }
        catch (IOException | SpimDataException exc) {
            throw new InternalServerErrorException(exc);
        }
    }

    @Query("HPCThumbnail")
    @Description("Return a thumbnail in base64 encoding")
    public DataReturn getThumbnail(@Name(UUID) String uuid,
           @Name(VERSION_PARAM) String version) throws IOException {
        ThumbnailProviderTS ts = getThumbnailProvider(uuid, version, thumbnailsGenerators, getSpimDataMinimalTS);
        return new DataReturn(
                DataReturn.ReturnType.BASE64,
                ts.runForThumbnail()
        );
    }

    @Query
    public DataReturn getSettingsXML()
    {
        throw new NotImplementedException();
    }
}
