package cz.it4i.fiji.datastore.register_service;

import cz.it4i.fiji.datastore.bdv_server.DataReturn;
import cz.it4i.fiji.datastore.core.DatasetDTO;
import cz.it4i.fiji.datastore.core.ViewRegistrationDTO;
import cz.it4i.fiji.datastore.core.ViewTransformDTO;
import cz.it4i.fiji.datastore.security.Authorization;
import lombok.*;
import lombok.extern.log4j.Log4j2;
import mpicbg.spim.data.SpimDataException;
import org.eclipse.microprofile.graphql.*;

import javax.inject.Inject;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static cz.it4i.fiji.datastore.DatasetServerEndpoint.*;
import static cz.it4i.fiji.datastore.register_service.DatasetRegisterServiceEndpoint.*;

@GraphQLApi
@Authorization
@Log4j2
public class DatasetRegisterServiceResource {

    @Inject
    DatasetRegisterServiceGQLImpl datasetRegisterServiceImpl;

    @Query("StartDataserver")
    @Description("Starts a data server with specified mode")
    public ConnectionParameters startDataServer(
            @Name(UUID) String uuid, @Name(R_X_PARAM) int rX,
            @Name(R_Y_PARAM) int rY, @Name(R_Z_PARAM) int rZ,
            @Name(VERSION_PARAM) String version, @Name(MODE_PARAM) String modeName,
            @Name(TIMEOUT_PARAM) long timeout
    ) throws Exception {
        log.info("Starting server for " + modeName + " dataset=" + uuid);
        OperationMode opMode = OperationMode.getByUrlPath(modeName);
        if (opMode == null || opMode == OperationMode.NOT_SUPPORTED)
            throw new Exception("Operation mode " + modeName + " not supported");

        try {
            ConnectionParameters serverURI = datasetRegisterServiceImpl.start(uuid, new int[]{rX, rY,
                    rZ}, version, opMode, timeout);
            log.debug("start reading> timeout = {}", timeout);
            return serverURI;
        } catch (IOException exc) {
            log.error("Starting server", exc);
            throw exc;
        }
    }

    @Query("StartWriteDataserver")
    @Description("Starts a data server for writing dataset")
    public ConnectionParameters startWriteDataserver(
            @Name(UUID) String uuid, @Name(R_X_PARAM) int rX,
            @Name(R_Y_PARAM) int rY, @Name(R_Z_PARAM) int rZ,
            @Name(RESOLUTION_PARAM) String resolutionString,
            @Name(TIMEOUT_PARAM) long timeout
    ) throws Exception {
        log.info("starting server for writing dataset={}", uuid);
        List<int[]> resolutions = getResolutions(rX, rY, rZ, resolutionString);
        try {
            ConnectionParameters serverURI = datasetRegisterServiceImpl.start(uuid, resolutions,
                    timeout);
            log.debug("start reading> timeout = {}", timeout);
            return serverURI;
        } catch (IOException exc) {
            log.error("Starting server", exc);
            throw exc;
        }
    }

    @Mutation("CreateEmptyDataset")
    @Description("Creates an empty dataset")
    public java.util.UUID createEmptyDataset(@Name("DatasetDTO") QLDatasetDTO dataset)
            throws SpimDataException, SystemException, IOException, NotSupportedException {
        log.info("creating empty dataset");
        log.debug("dataset=" + dataset);
        try {
            return datasetRegisterServiceImpl.createEmptyDataset(getDatasetDTO(dataset));
        } catch (Exception exc) {
            log.warn("read", exc);
            throw exc;
        }
    }

    @Mutation("AddExistingDataset")
    @Description("Adds an existing dataset")
    public DataReturn addExistingDataset(@Name(UUID) String uuid) throws Exception {
        log.info("adding existing dataset {}", uuid);
        try {
            datasetRegisterServiceImpl.addExistingDataset(uuid);
        } catch (IOException exc) {
            throw new NotFoundException("Dataset with uuid " + uuid +
                    "  was not located in storage ");
        } catch (DatasetAlreadyInsertedException exc) {
            throw new Exception("Dataset with uuid " + exc.getUuid() + " is already added.");
        } catch (Exception exc) {
            throw new InternalServerErrorException("Cannot add dataset " + uuid);
        }
        return new DataReturn(DataReturn.ReturnType.SUCCESS, null);
    }

    @Query("QueryDataset")
    @Description("Returns a dataset")
    public QLDatasetDTO queryDataset(@Name(UUID) String uuid) {
        log.info("get JSON for dataset={}", uuid);
        try {
            return new QLDatasetDTO(datasetRegisterServiceImpl.query(uuid));
        } catch (SpimDataException exc) {
            throw new InternalServerErrorException("Query to dataset failed", exc);
        }
    }

    @Mutation("DeleteDataset")
    @Description("Deletes a dataset")
    public DataReturn deleteDataset(@Name(UUID) String uuid) {
        log.info("deleting dataset={}", uuid);
        try {
            datasetRegisterServiceImpl.deleteDataset(uuid);
            return new DataReturn(DataReturn.ReturnType.SUCCESS, null);
        } catch (Exception exc) {
            log.error("deleteDataset", exc);
            throw new InternalServerErrorException("Cannot delete dataset " + uuid);
        }
    }

    @Mutation("DeleteDatasetVersions")
    public DataReturn deleteDatasetVersions(
            @Name(UUID) String uuid,
            @Name(VERSION_PARAM) String version,
            @Name(VERSION_PARAMS) String versions
    ) throws IOException {
        log.info("deleting versions from dataset={}", uuid);
        List<Integer> versionList = getVersions(version, versions);
        try {
            datasetRegisterServiceImpl.deleteVersions(uuid, versionList);
            return new DataReturn(DataReturn.ReturnType.SUCCESS, null);
        } catch (IOException exc) {
            log.error("deleteDatasetVersions", exc);
            throw new InternalServerErrorException("Cannot delete dataset versions" + uuid);
        }
    }

    @Query("GetCommonMetadata")
    @Description("Returns common metadata")
    public DataReturn getCommonMetadata(@Name(UUID) String uuid) {
        log.info("getting common metadata from dataset={}", uuid);
        return new DataReturn(
                DataReturn.ReturnType.TEXT,
                datasetRegisterServiceImpl.getCommonMetadata(uuid)
        );
    }

    @Mutation("SetCommonMetadata")
    @Description("Sets metadata")
    public DataReturn setCommonMetadata(@Name(UUID) String uuid, @Name("metadata") String metadata) {
        log.info("setting common metadata into dataset={}", uuid);
        datasetRegisterServiceImpl.setCommonMetadata(uuid, metadata);
        return new DataReturn(DataReturn.ReturnType.SUCCESS, null);
    }

    @Mutation("AddChannels")
    @Description("Add channels to dataset")
    public DataReturn addChannels(@Name(UUID) String uuid, @Name(CHANNEL_PARAM) String strChannels) {
        try {
            int channels = strChannels.isEmpty() ? 1 : Integer.parseInt(strChannels);
            log.info("add channels {} for dataset={}", channels, uuid);
            datasetRegisterServiceImpl.addChannels(uuid, channels);
            return new DataReturn(DataReturn.ReturnType.SUCCESS, null);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(strChannels + " is not integer");
        } catch (Exception exc) {
            log.warn("read", exc);
            throw new InternalServerErrorException("Cannot add channels " + uuid);
        }
    }

    @Query("GetChannels")
    @Description("Get channels from dataset")
    public QLDatasetDTO getChannels(@Name(UUID) String uuid) throws Exception {
        try {
            cz.it4i.fiji.datastore.core.DatasetDTO result = datasetRegisterServiceImpl.query(uuid);
            if (result == null) throw new Exception("Dataset with uuid=" + uuid + " not found");
            return new QLDatasetDTO(result);
        } catch (SpimDataException exc) {
            throw new InternalServerErrorException("Query to dataset failed", exc);
        }
    }

    @Mutation("Rebuild")
    @Description("Rebuilds a dataset")
    public DataReturn rebuildDataset(@Name(UUID) String uuid,
                                     @Name(VERSION_PARAM) int version, @Name(TIME_PARAM) int time,
                                     @Name(CHANNEL_PARAM) int channel, @Name(ANGLE_PARAM) int angle
    ) {
        try {
            datasetRegisterServiceImpl.rebuild(uuid, version, time, channel, angle);
            return new DataReturn(DataReturn.ReturnType.SUCCESS, null);
        } catch (SpimDataException | IOException exc) {
            log.error("rebuild", exc);
            throw new InternalServerErrorException(
                    "Rebuild failure. Contact administrator");
        }
    }

    @Type
    @Getter
    @Setter
    @NoArgsConstructor
    public static class QLDatasetDTO {
        private String uuid;
        private String voxelType;
        private long[] dimensions;
        private int timepoints;
        private int channels;
        private int angles;
        private List<List<Double>> transformations;
        private String voxelUnit;
        private double[] voxelResolution;
        private QLResolution timepointResolution;
        private QLResolution channelResolution;
        private QLResolution angleResolution;
        private String compression;
        private QLResolutionLevel[] resolutionLevels;
        private List<Integer> versions;
        private String label;
        private List<QLViewRegistrationDTO> viewRegistrations;
        private List<Integer> timepointIds;
        private String datasetType;

        public QLDatasetDTO(DatasetDTO datasetDTO) {
            if (datasetDTO == null) {
                throw new IllegalArgumentException("DatasetDTO cannot be null"); // Important null check
            }
            this.uuid = datasetDTO.getUuid(); // Or datasetDTO.uuid if it's public
            this.voxelType = datasetDTO.getVoxelType();
            this.dimensions = datasetDTO.getDimensions();
            this.timepoints = datasetDTO.getTimepoints();
            this.channels = datasetDTO.getChannels();
            this.angles = datasetDTO.getAngles();
            double[][] originalTransformations = datasetDTO.getTransformations();
            if (originalTransformations != null) {
                this.transformations = new ArrayList<>();
                for (double[] row : originalTransformations) {
                    this.transformations.add(Arrays.stream(row)
                            .boxed()  // Convert double to Double
                            .collect(Collectors.toList())); // Collect to a List<Double>
                }
            } else {
                this.transformations = null;
            }
            this.voxelUnit = datasetDTO.getVoxelUnit();
            this.voxelResolution = datasetDTO.getVoxelResolution();
            this.timepointResolution = new QLResolution(datasetDTO.getTimepointResolution());
            this.channelResolution = new QLResolution(datasetDTO.getChannelResolution());
            this.angleResolution = new QLResolution(datasetDTO.getAngleResolution());
            this.compression = datasetDTO.getCompression();
            DatasetDTO.ResolutionLevel[] originalResolutionLevels = datasetDTO.getResolutionLevels();
            if (originalResolutionLevels != null) {
                this.resolutionLevels = new QLResolutionLevel[originalResolutionLevels.length];
                for (int i = 0; i < originalResolutionLevels.length; i++) {
                    this.resolutionLevels[i] = new QLResolutionLevel(originalResolutionLevels[i]);
                }
            } else {
                this.resolutionLevels = null;
            }            this.versions = datasetDTO.getVersions();
            this.label = datasetDTO.getLabel();
            this.viewRegistrations = new ArrayList<QLViewRegistrationDTO>();
            for (ViewRegistrationDTO vdto: datasetDTO.getViewRegistrations()) {
                this.viewRegistrations.add(new QLViewRegistrationDTO(vdto));
            }
            this.timepointIds = datasetDTO.getTimepointIds();
            this.datasetType = datasetDTO.getDatasetType();
        }

        @Type
        @NoArgsConstructor
        @AllArgsConstructor
        @Getter
        @Setter
        public static class QLResolution {
            double value;
            String unit;

            public QLResolution(DatasetDTO.Resolution resolution) {
                this.value = resolution.getValue();
                this.unit = resolution.getUnit();
            }

            public DatasetDTO.Resolution toResolution() {
                return new DatasetDTO.Resolution(this.value, this.unit);
            }
        }

        @Type
        @NoArgsConstructor
        @AllArgsConstructor
        @Getter
        @Setter
        public static class QLResolutionLevel {
            int[] resolutions;
            int[] blockDimensions;

            public QLResolutionLevel(DatasetDTO.ResolutionLevel resolutionLevel) {
                this.blockDimensions = resolutionLevel.getBlockDimensions();
                this.resolutions = resolutionLevel.getResolutions();
            }
        }

        @Type
        @NoArgsConstructor
        @AllArgsConstructor
        @Getter
        @Setter
        public static class QLViewRegistrationDTO {
            private int angle;
            private int channel;
            private int time;
            private List<QLViewTransformDTO> transformations;

            public QLViewRegistrationDTO(ViewRegistrationDTO dto) {
                this.angle = dto.getAngle();
                this.channel = dto.getChannel();
                this.time = dto.getTime();
                this.transformations = new ArrayList<>();
                for (ViewTransformDTO dtoTransform : dto.getTransformations()) {
                    this.transformations.add(new QLViewTransformDTO(dtoTransform));
                }
            }
        }

        @Type
        @NoArgsConstructor
        @AllArgsConstructor
        @Getter
        @Setter
        public static class QLViewTransformDTO {
            private String name;
            private double[] rowPackedMatrix;

            public QLViewTransformDTO(ViewTransformDTO dto) {
                this.name = dto.getName();
                this.rowPackedMatrix = dto.getRowPackedMatrix();
            }
        }

    }

    public DatasetDTO getDatasetDTO(QLDatasetDTO dto) {

        DatasetDTO.ResolutionLevel[] originalResolutionLevels = dto.resolutionLevels != null ? new DatasetDTO.ResolutionLevel[dto.resolutionLevels.length] : null;

        if (dto.resolutionLevels != null) {
            for (int i = 0; i < dto.resolutionLevels.length; i++) {
                originalResolutionLevels[i] = toResolutionLevel(dto.resolutionLevels[i]);
            }
        }

        ArrayList<ViewRegistrationDTO> viewRegistrations = new ArrayList<ViewRegistrationDTO>();

        if (dto.viewRegistrations != null)
            for (QLDatasetDTO.QLViewRegistrationDTO vdto: dto.viewRegistrations) {
                viewRegistrations.add(toViewRegistrationDTO(vdto));
            }

        return new DatasetDTO(
                dto.uuid,
                dto.voxelType,
                dto.dimensions != null ? Arrays.copyOf(dto.dimensions, dto.dimensions.length) : null,
                dto.timepoints,
                dto.channels,
                dto.angles,
                convertListOfListsToArray(dto.transformations), // Convert List<List<Double>> to double[][]
                dto.voxelUnit,
                dto.voxelResolution != null ? Arrays.copyOf(dto.voxelResolution, dto.voxelResolution.length) : null,
                dto.timepointResolution.toResolution(),
                dto.channelResolution.toResolution(),
                dto.angleResolution.toResolution(),
                dto.compression,
                originalResolutionLevels,
                dto.versions != null ? new ArrayList<>(dto.versions) : null,
                dto.label,
                viewRegistrations.isEmpty() ? null : viewRegistrations,
                dto.timepointIds != null ? new ArrayList<>(dto.timepointIds) : null,
                dto.datasetType
        );
    }

    public DatasetDTO.ResolutionLevel toResolutionLevel(QLDatasetDTO.QLResolutionLevel dto) {
        return new DatasetDTO.ResolutionLevel(dto.getBlockDimensions(), dto.getResolutions());
    }

    public ViewRegistrationDTO toViewRegistrationDTO(QLDatasetDTO.QLViewRegistrationDTO dto) {
        ArrayList<ViewTransformDTO> views = new ArrayList<>();
        for (QLDatasetDTO.QLViewTransformDTO x : dto.transformations)
            views.add(toViewTransformDTO(x));

        return new ViewRegistrationDTO(dto.angle, dto.channel, dto.time, views);
    }

    public ViewTransformDTO toViewTransformDTO(QLDatasetDTO.QLViewTransformDTO dto) {
        return new ViewTransformDTO(dto.name, dto.rowPackedMatrix);
    }

    private double[][] convertListOfListsToArray(List<List<Double>> listOfLists) {
        if (listOfLists == null) {
            return null;
        }

        double[][] array = new double[listOfLists.size()][];
        for (int i = 0; i < listOfLists.size(); i++) {
            List<Double> row = listOfLists.get(i);
            if (row != null) {  // Handle null rows
                array[i] = row.stream().mapToDouble(Double::doubleValue).toArray();
            } else {
                array[i] = null; // Or handle as needed
            }
        }
        return array;
    }
}