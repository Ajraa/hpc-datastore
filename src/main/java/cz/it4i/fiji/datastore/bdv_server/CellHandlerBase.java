package cz.it4i.fiji.datastore.bdv_server;

import bdv.img.cache.VolatileGlobalCellCache;
import bdv.img.hdf5.Hdf5ImageLoader;
import bdv.img.n5.BdvN5Format;
import bdv.img.remote.AffineTransform3DJsonSerializer;
import bdv.img.remote.RemoteImageLoader;
import bdv.spimdata.SpimDataMinimal;
import bdv.spimdata.XmlIoSpimDataMinimal;
import com.google.gson.GsonBuilder;
import cz.it4i.fiji.datastore.DatasetHandler;
import cz.it4i.fiji.datastore.core.DatasetDTO;
import cz.it4i.fiji.datastore.core.HPCDatastoreImageLoaderMetaData;
import cz.it4i.fiji.datastore.register_service.Dataset;
import cz.it4i.fiji.datastore.register_service.DatasetAssembler;
import lombok.extern.log4j.Log4j2;
import mpicbg.spim.data.SpimDataException;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.cell.Cell;
import net.imglib2.realtransform.AffineTransform3D;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static cz.it4i.fiji.datastore.ApplicationConfiguration.BASE_NAME;
import static cz.it4i.fiji.datastore.bdv_server.SpimDataMapper.asSpimDataMinimal;

@Log4j2
public abstract class CellHandlerBase<T> {

    static class Key
    {
        private final int timepoint;

        private final int setup;

        private final int level;

        private final long index;

        private final String[] parts;

        /**
         * Create a Key for the specified cell. Note that {@code cellDims} and
         * {@code cellMin} are not used for {@code hashcode()/equals()}.
         *
         * @param timepoint
         *            timepoint coordinate of the cell
         * @param setup
         *            setup coordinate of the cell
         * @param level
         *            level coordinate of the cell
         * @param index
         *            index of the cell (flattened spatial coordinate of the
         *            cell)
         */


        /**
         * Key for a cell identified by timepoint, setup, level, and index
         * (flattened spatial coordinate).
         */

        Key(final int timepoint, final int setup, final int level, final long index,
            final String[] parts)
        {
            this.timepoint = timepoint;
            this.setup = setup;
            this.level = level;
            this.index = index;
            this.parts = parts;

            int value = Long.hashCode( index );
            value = 31 * value + level;
            value = 31 * value + setup;
            value = 31 * value + timepoint;
            hashcode = value;
        }

        @Override
        public boolean equals( final Object other )
        {
            if ( this == other )
                return true;
            if ( !( other instanceof VolatileGlobalCellCache.Key ) )
                return false;
            final Key that = ( Key ) other;
            return ( this.index == that.index ) && ( this.timepoint == that.timepoint ) && ( this.setup == that.setup ) && ( this.level == that.level );
        }

        final int hashcode;

        @Override
        public int hashCode()
        {
            return hashcode;
        }
    }

    CellHandlerBase(DatasetHandler datasetHandler,
                  Supplier<Dataset> datasetSupplier,
                  final String baseUrl, int version, final String datasetName,
                  final String thumbnailsDirectory) throws IOException
    {
        this.baseUrl = baseUrl;
        this.datasetSupplier = datasetSupplier;
        this.spimdataSupplier = () -> getSpimData(datasetHandler, version);


        final N5Writer writer = 0 <= version ? datasetHandler.getWriter(version) : datasetHandler
                .constructChainOfWriters();
        final Map<String, DatasetAttributes> perPathDatasetAttribute =
                new HashMap<>();

        loader = key -> {
            final int[] cellDims = new int[] {
                    Integer.parseInt( key.parts[ 5 ] ),
                    Integer.parseInt( key.parts[ 6 ] ),
                    Integer.parseInt( key.parts[ 7 ] ) };
            final long[] cellMin = new long[] {
                    Long.parseLong( key.parts[ 8 ] ),
                    Long.parseLong( key.parts[ 9 ] ),
                    Long.parseLong( key.parts[ 10 ] ) };
            DataBlock<?> block = readBlock(perPathDatasetAttribute, writer, key,
                    cellDims, cellMin);
            return new Cell<>(cellDims, cellMin, block);

        };

        cache = new SoftRefLoaderCache<>();

        // dataSetURL property is used for providing the XML file by replace
        // SequenceDescription>ImageLoader>baseUrl
        baseFilename = BASE_NAME;
        settingsXmlString = buildSettingsXML( baseFilename );
        thumbnailProviderTS = new ThumbnailProviderTS(spimdataSupplier.get(),
                datasetName, thumbnailsDirectory);
    }

    protected final CacheLoader< Key, Cell< ? >> loader;

    protected final LoaderCache< Key, Cell< ? > > cache;


    /**
     * Full path of the dataset xml file this {@link CellHandlerTS} is serving,
     * without the ".xml" suffix.
     */
    private final String baseFilename;



    /**
     * Cached dataset.settings XML to be send to clients. May be null if no
     * settings file exists for the dataset.
     */
    protected final String settingsXmlString;

    protected final ThumbnailProviderTS thumbnailProviderTS;

    protected final String baseUrl;

    protected final Supplier<Dataset> datasetSupplier;

    protected Supplier<SpimDataMinimal> spimdataSupplier;

    public abstract T runForCellOrInit(final String cellString);
    public abstract T runForDataset();
    public abstract T runForSettings();

    public DatasetAttributes getDatasetAttributes(
            Map<String, DatasetAttributes> perPathDatasetAttribute, N5Writer writer,
            String path)
    {
        synchronized (perPathDatasetAttribute) {
            return perPathDatasetAttribute.computeIfAbsent(path, x -> {
                try {
                    return writer.getDatasetAttributes(path);
                }
                catch (IOException exc) {
                    throw new RuntimeException(exc);
                }
            });
        }
    }

    HPCDatastoreImageLoaderMetaData metadata = null;

    private DataBlock<?> readBlock(
            Map<String, DatasetAttributes> perPathDatasetAttribute, N5Writer writer,
            Key key, int[] cellDims, long[] cellMin) throws IOException
    {
        String path = BdvN5Format.getPathName(key.setup, key.timepoint, key.level);
        DatasetAttributes datasetAttributes = getDatasetAttributes(perPathDatasetAttribute, writer, path);
        final int[][] blockSizes = metadata.getPerSetupMipmapInfo().get(key.setup).getSubdivisions();
        long[] gridPosition = new long[cellMin.length];
        for (int i = 0; i < gridPosition.length; i++) {
			/*if (levelBlockSize[i] != cellDims[i] || cellMin[i] % cellDims[i] != 0) {
				throw new IllegalArgumentException(String.format(
					"Invalid block dimension of coordinates levelBlockSize=%s min=%s dims=%s",
					Arrays.toString(levelBlockSize), Arrays.toString(cellMin), Arrays
						.toString(cellDims)));
			}*/
            gridPosition[i] = cellMin[i] / blockSizes[key.level][i];
        }
        DataBlock<?> result = writer.readBlock(path, datasetAttributes,
                gridPosition);

        return result;
    }

    /**
     * Create a JSON representation of the {@link HPCDatastoreImageLoaderMetaData}
     * (image sizes and resolutions) provided by the given
     * {@link Hdf5ImageLoader}.
     *
     */
    protected static String buildMetadataJsonString(SpimDataMinimal spimData,
                                                  Dataset dataset, HPCDatastoreImageLoaderMetaData[] metaDataRetPtr)
    {
        final DatasetDTO datasetDTO = DatasetAssembler.createDatatransferObject(
                dataset, spimData.getSequenceDescription().getTimePoints());
        metaDataRetPtr[0] =
                new HPCDatastoreImageLoaderMetaData(datasetDTO, spimData
                        .getSequenceDescription(), DataType.fromString(dataset.getVoxelType()));
        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter( AffineTransform3D.class, new AffineTransform3DJsonSerializer() );
        gsonBuilder.enableComplexMapKeySerialization();
        return gsonBuilder.create().toJson( metaDataRetPtr[0] );
    }

    protected static String buildRemoteDatasetXML(XmlIoSpimDataMinimal io,
                                                SpimDataMinimal spimData, String baseUrl) throws IOException,
            SpimDataException
    {
        StringWriter sw = new StringWriter();
        BuildRemoteDatasetXmlTS.run(io, spimData, new RemoteImageLoader(baseUrl,
                false), sw);
        return sw.toString();
    }



    private static SpimDataMinimal getSpimData(DatasetHandler datasetHandler,
                                               int version)
    {
        try {
            return asSpimDataMinimal(datasetHandler.getSpimData(version));
        }
        catch (SpimDataException exc) {
            throw new RuntimeException(exc);
        }
    }

    /**
     * Read {@code baseFilename.settings.xml} into a string if it exists.
     *
     * @return contents of {@code baseFilename.settings.xml} or {@code null} if
     *         that file couldn't be read.
     */
    private static String buildSettingsXML( final String baseFilename )
    {
        final String settings = baseFilename + ".settings.xml";
        if ( new File( settings ).exists() )
        {
            try
            {
                final SAXBuilder sax = new SAXBuilder();
                final Document doc = sax.build( settings );
                final XMLOutputter xout = new XMLOutputter( Format.getPrettyFormat() );
                final StringWriter sw = new StringWriter();
                xout.output( doc, sw );
                return sw.toString();
            }
            catch (JDOMException | IOException e )
            {
                log.warn("Could not read settings file \"" + settings + "\"");
                log.warn(e.getMessage());
            }
        }
        return null;
    }
}
