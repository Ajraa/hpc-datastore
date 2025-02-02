package cz.it4i.fiji.datastore.register_service;

import cz.it4i.fiji.datastore.core.DatasetDTO;
import cz.it4i.fiji.datastore.security.Authorization;
import lombok.extern.log4j.Log4j2;
import mpicbg.spim.data.SpimDataException;
import org.eclipse.microprofile.graphql.*;

import javax.inject.Inject;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static cz.it4i.fiji.datastore.DatasetServerEndpoint.*;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.*;

@GraphQLApi
@Authorization
@Log4j2
public class DatasetRegisterServiceResource {

    @Inject
    DatasetRegisterServiceImpl datasetRegisterServiceImpl;

    @Query("StartDataserver")
    @Description("Starts a data server with specified mode")
    public URI startDataServer(
            @Name(UUID) String uuid, @Name(R_X_PARAM) int rX,
            @Name(R_Y_PARAM) int rY, @Name(R_Z_PARAM) int rZ,
            @Name(VERSION_PARAM) String version, @Name(MODE_PARAM) String modeName,
            @Name(TIMEOUT_PARAM) long timeout
            ) throws Exception {
        log.info("Starting server for " + modeName + " dataset=" + uuid);
        OperationMode opMode = OperationMode.getByUrlPath(modeName);
        if (opMode == null || opMode == OperationMode.NOT_SUPPORTED) throw new Exception("Operation mode " + modeName + " not supported");

        try {
            URI serverURI = datasetRegisterServiceImpl.start(uuid, new int[] { rX, rY,
                    rZ }, version, opMode, timeout);
            log.debug("start reading> timeout = {}", timeout);
            return serverURI;
        }
        catch (IOException exc) {
            log.error("Starting server", exc);
            throw exc;
        }
    }

    @Query("StartWriteDataserver")
    @Description("Starts a data server for writing dataset")
    public URI startWriteDataserver(
            @Name(UUID) String uuid, @Name(R_X_PARAM) int rX,
            @Name(R_Y_PARAM) int rY, @Name(R_Z_PARAM) int rZ,
            @Name(RESOLUTION_PARAM) String resolutionString,
            @Name(TIMEOUT_PARAM) long timeout
    ) throws Exception {
        log.info("starting server for writing dataset=" + uuid);
        List<int[]> resolutions = getResolutions(rX, rY, rZ, resolutionString);
        try {
            URI serverURI = datasetRegisterServiceImpl.start(uuid, resolutions,
                    timeout);
            log.debug("start reading> timeout = {}", timeout);
            return serverURI;
        }
        catch (IOException exc) {
            log.error("Starting server", exc);
            throw exc;
        }
    }

    @Mutation("CreateEmptyDataset")
    @Description("Creates an empty dataset")
    public java.util.UUID createEmptyDataset(@Name("DatasetDTO") DatasetDTO dataset)
            throws SpimDataException, SystemException, IOException, NotSupportedException {
        log.info("creating empty dataset");
        log.debug("dataset=" + dataset);
        try {
            return datasetRegisterServiceImpl.createEmptyDataset(
                    dataset);
        }
        catch (Exception exc) {
            log.warn("read", exc);
            throw exc;
        }
    }

    @Mutation("AddExistingDataset")
    @Description("Adds an existing dataset")
    public String addExistingDataset(@Name(UUID) String uuid) throws Exception {
        log.info("adding existing dataset {}", uuid);
        try {
            datasetRegisterServiceImpl.addExistingDataset(uuid);
        }
        catch (IOException exc) {
            throw new NotFoundException("Dataset with uuid " + uuid +
                    "  was not located in storage ");
        }
        catch (DatasetAlreadyInsertedException exc) {
            throw new Exception("Dataset with uuid " + exc.getUuid() + " is already added.");
        }
        catch (Exception exc) {
            throw new InternalServerErrorException("Cannot add dataset " + uuid);
        }
        return "Done";
    }

    @Query("QueryDataset")
    @Description("Returns a dataset")
    public DatasetDTO queryDataset(@Name(UUID) String uuid) {
        log.info("get JSON for dataset=" + uuid);
        try {
            return datasetRegisterServiceImpl.query(uuid);
        }
        catch (SpimDataException exc) {
            throw new InternalServerErrorException("Query to dataset failed", exc);
        }
    }

    @Mutation("DeleteDataset")
    @Description("Deletes a dataset")
    public void deleteDataset(@Name(UUID) String uuid) {
        log.info("deleting dataset=" + uuid);
        try {
            datasetRegisterServiceImpl.deleteDataset(uuid);
        }
        catch (Exception exc) {
            log.error("deleteDataset", exc);
            throw new InternalServerErrorException("Cannot delete dataset " + uuid);
        }
    }

    @Mutation("DeleteDatasetVersions")
    public void deleteDatasetVersions(
            @Name(UUID) String uuid,
            @Name(VERSION_PARAM) String version,
            @Name(VERSION_PARAMS) String versions
    ) throws IOException {
        log.info("deleting versions from dataset=" + uuid);
        List<Integer> versionList = getVersions(version, versions);
        try {
            datasetRegisterServiceImpl.deleteVersions(uuid, versionList);
        }
        catch (IOException exc) {
            log.error("deleteDatasetVersions", exc);
            throw new InternalServerErrorException("Cannot delete dataset versions" + uuid);
        }
    }

    @Query("GetCommonMetadata")
    @Description("Returns common metadata")
    public String getCommonMetadata(@Name(UUID) String uuid) {
        log.info("getting common metadata from dataset=" + uuid);
        return datasetRegisterServiceImpl.getCommonMetadata(uuid);
    }

    @Mutation("SetCommonMetadata")
    @Description("Sets metadata")
    public void setCommonMetadata(@Name(UUID) String uuid, @Name("metadata") String metadata) {
        log.info("setting common metadata into dataset=" + uuid);
        datasetRegisterServiceImpl.setCommonMetadata(uuid, metadata);
    }

    @Mutation("AddChannels")
    @Description("Add channels to dataset")
    public void addChannels(@Name(UUID) String uuid, @Name("channels") String strChannels) {
        try {
            int channels = strChannels.isEmpty() ? 1 : Integer.parseInt(strChannels);
            log.info("add channels " + channels + " for dataset=" + uuid);
            datasetRegisterServiceImpl.addChannels(uuid, channels);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(strChannels + " is not integer");
        }
        catch (Exception exc) {
            log.warn("read", exc);
            throw new InternalServerErrorException("Cannot add channels " + uuid);
        }
    }

    @Query("GetChannels")
    @Description("Get channels from dataset")
    public DatasetDTO getChannels(@Name(UUID) String uuid) throws Exception {
        try {
            DatasetDTO result = datasetRegisterServiceImpl.query(uuid);
            if (result == null) throw new Exception("Dataset with uuid=" + uuid + " not found");
            return result;
        }
        catch (SpimDataException exc) {
            throw new InternalServerErrorException("Query to dataset failed", exc);
        }
    }

    @Mutation("Rebuild")
    @Description("Rebuilds a dataset")
    public void rebuildDataset(@Name(UUID) String uuid,
        @Name(VERSION_PARAM) int version, @Name(TIME_PARAM) int time,
        @Name(CHANNEL_PARAM) int channel, @Name(ANGLE_PARAM) int angle
    )
    {
        try {
            datasetRegisterServiceImpl.rebuild(uuid, version, time, channel, angle);
        } catch (SpimDataException | IOException exc) {
            log.error("rebuild", exc);
            throw new InternalServerErrorException(
                    "Rebuild failure. Contact administrator");
        }
    }
}












































