package cz.it4i.fiji.datastore.bdv_server;

import bdv.spimdata.SpimDataMinimal;
import mpicbg.spim.data.SpimDataException;

import javax.ws.rs.InternalServerErrorException;
import java.io.IOException;
import java.util.Map;

public class HPCDatastoreHelper {
    public static ThumbnailProviderTS getThumbnailProvider(String uuid,
                                                     String version,
                                                     Map<String, ThumbnailProviderTS> thumbnailsGenerators,
                                                     GetSpimDataMinimalTS getSpimDataMinimalTS)
    {
        String key = getKey(uuid, version);
        return thumbnailsGenerators.computeIfAbsent(key,
                x -> {
                    try {
                        return constructThumbnailGeneratorTS(uuid, version, getSpimDataMinimalTS);
                    }
                    catch (SpimDataException | IOException exc) {
                        throw new InternalServerErrorException(exc);
                    }
                });
    }

    private static ThumbnailProviderTS constructThumbnailGeneratorTS(String uuid,
                                                              String version,
                                                              GetSpimDataMinimalTS getSpimDataMinimalTS) throws SpimDataException, IOException
    {
        // thumbnail is done from mixedLatest version as it requires transform
        // setups in
        // N5Reader and get N5Reader base on setupID
        version = "mixedLatest";
        SpimDataMinimal spimData = getSpimDataMinimalTS.run(uuid, version);
        return new ThumbnailProviderTS(spimData, uuid + "_version-" + version,
                GetThumbnailsDirectoryTS.$());
    }

    private static String getKey(String uuid, String version) {
        return uuid + ":" + version;
    }

}
