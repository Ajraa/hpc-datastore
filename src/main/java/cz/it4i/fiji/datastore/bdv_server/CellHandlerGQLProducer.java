package cz.it4i.fiji.datastore.bdv_server;

import cz.it4i.fiji.datastore.ApplicationConfiguration;
import cz.it4i.fiji.datastore.core.Version;
import cz.it4i.fiji.datastore.register_service.DatasetRepository;
import cz.it4i.fiji.datastore.register_service.WriteToVersionListener;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static cz.it4i.fiji.datastore.core.Version.stringToIntVersion;

@ApplicationScoped
public class CellHandlerGQLProducer implements WriteToVersionListener {
    @Inject
    DatasetRepository repository;

    @Inject
    ApplicationConfiguration configuration;

    final private Map<String, CellHandlerGQL> cellHandlersGQL = new HashMap<>();

    synchronized CellHandlerGQL produce(URI baseURI, String uuidStr,
                                       final String versionStr)
    {
        final int version = stringToIntVersion(versionStr);
        String key = getKey(uuidStr, versionStr);
        String baseURL = baseURI.resolve("bdv/").resolve(uuidStr + "/").resolve(
                versionStr).toString();
        return cellHandlersGQL.computeIfAbsent(key, x -> create(baseURL, uuidStr,
                version));
    }

    @Override
    synchronized public void writingToVersion(String uuidStr, int version) {
        cellHandlersGQL.remove(getKey(uuidStr, "" + version));
        clearCacheForMixedLatest(uuidStr);
    }

    @Override
    synchronized public void writeToAllVersions(String uuid) {
        String keyPrefix = getKey(uuid, "");
        for (Iterator<Map.Entry<String, CellHandlerGQL>> iter = cellHandlersGQL.entrySet()
                .iterator(); iter.hasNext();)
        {
            Map.Entry<String, CellHandlerGQL> entry = iter.next();
            if (entry.getKey().startsWith(keyPrefix)) {
                iter.remove();
            }
        }
        clearCacheForMixedLatest(uuid);
    }

    private String getKey(String uuid, String version) {
        return uuid + ":" + version;
    }

    private void clearCacheForMixedLatest(String uuidStr) {
        cellHandlersGQL.remove(getKey(uuidStr, Version.MIXED_LATEST_VERSION_NAME));
    }

    private CellHandlerGQL create(String baseURL, String uuid, int version) {

        // only for check that version exists
        repository.findByUUIDVersion(uuid, version);

        try {
            return new CellHandlerGQL(configuration.getDatasetHandler(uuid), () ->repository.findByUUID(uuid),
                    baseURL, version, uuid + "_version-" + version, GetThumbnailsDirectoryTS
                    .$());
        }
        catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

}
