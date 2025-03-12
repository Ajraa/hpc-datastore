package cz.it4i.fiji.datastore.register_service;

import cz.it4i.fiji.datastore.bdv_server.CellHandlerGQLProducer;
import cz.it4i.fiji.datastore.management.DataServerManager;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequestScoped
public class DatasetRegisterServiceGQLImpl extends DatasetRegisterServiceImplBase<ConnectionParameters> {

    @Inject
    CellHandlerGQLProducer writeToVersionListener;

    @Inject
    protected DataServerManager dataServerManager;

    @Override
    public ConnectionParameters start(String uuid, int[] r, String version, OperationMode mode,
                     Long timeout) throws IOException
    {

        Dataset dataset = getDataset(uuid);
        if (null == dataset.getBlockDimension(r)) {
            throw new NotFoundException("Dataset with UUID=" + uuid +
                    " has not resolution [" + IntStream.of(r).mapToObj(i -> "" + i).collect(
                    Collectors.joining(",")) + "]");
        }
        int resolvedVersion = resolveVersion(dataset, version, mode);
        if (mode.allowsWrite()) {
            writeToVersionListener.writingToVersion(uuid, resolvedVersion);
        }
        return dataServerManager.startDataServer(dataset.getUuid(), r,
                resolvedVersion, version.equals("mixedLatest"), mode, timeout);
    }

    @Override
    public ConnectionParameters start(String uuid, List<int[]> resolutions, Long timeout)
            throws IOException
    {
        Dataset dataset = getDataset(uuid);
        // called only for checking that all resolutions exists
        getNonIdentityResolutions(dataset, resolutions);
        mergeVersions(dataset);
        writeToVersionListener.writeToAllVersions(uuid);
        return dataServerManager.startDataServer(dataset.getUuid(), resolutions,
                timeout);

    }
}
