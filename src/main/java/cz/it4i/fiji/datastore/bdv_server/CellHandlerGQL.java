package cz.it4i.fiji.datastore.bdv_server;

import bdv.spimdata.XmlIoSpimDataMinimal;
import cz.it4i.fiji.datastore.DatasetHandler;
import cz.it4i.fiji.datastore.core.HPCDatastoreImageLoaderMetaData;
import cz.it4i.fiji.datastore.register_service.Dataset;
import lombok.extern.log4j.Log4j2;
import mpicbg.spim.data.SpimDataException;
import net.imglib2.img.cell.Cell;
import org.janelia.saalfeldlab.n5.DataBlock;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Log4j2
public class CellHandlerGQL extends CellHandlerBase<DataReturn>{
    CellHandlerGQL(DatasetHandler datasetHandler, Supplier<Dataset> datasetSupplier, String baseUrl, int version, String datasetName, String thumbnailsDirectory) throws IOException {
        super(datasetHandler, datasetSupplier, baseUrl, version, datasetName, thumbnailsDirectory);
    }

    @Override
    public DataReturn runForCellOrInit(final String cellString) {
        final String[] parts = cellString.split("/");
        if (parts[0].equals("cell"))
        {
            final int index = Integer.parseInt( parts[ 1 ] );
            final int timepoint = Integer.parseInt( parts[ 2 ] );
            final int setup = Integer.parseInt( parts[ 3 ] );
            final int level = Integer.parseInt( parts[ 4 ] );
            final Key key = new Key( timepoint, setup, level, index, parts );
            // TODO - there should be another type
            byte[] data;
            try
            {
                final Cell< ? > cell = cache.get( key, loader );
                DataBlock<short[]> dataBlock = (DataBlock<short[]>) cell.getData();
                if (dataBlock == null) {
                    data = new byte[0];
                }
                else {
                    data = dataBlock.toByteBuffer().array();
                }
            }
            catch ( ExecutionException e )
            {
                log.error("getData", e);
                data = new byte[0];
            }
            return new DataReturn(
                    DataReturn.ReturnType.BASE64,
                    Base64.getEncoder().encodeToString(data)
            );
        }
        else if (parts[0].equals("init"))
        {
            HPCDatastoreImageLoaderMetaData[] metadataArray = { null };
            return new DataReturn(
                    DataReturn.ReturnType.JSON,
                    buildMetadataJsonString(spimdataSupplier.get(), datasetSupplier.get(), metadataArray)
            );
        }
        throw new RuntimeException("Bad request");
    }

    @Override
    public DataReturn runForDataset() {
        final XmlIoSpimDataMinimal io = new XmlIoSpimDataMinimal();

        try {
            return new DataReturn(
                    DataReturn.ReturnType.XML,
                    buildRemoteDatasetXML(io, spimdataSupplier.get(), baseUrl)
            );

        }
        catch (IOException | SpimDataException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public DataReturn runForSettings() {
        if (settingsXmlString != null)
            return new DataReturn(
              DataReturn.ReturnType.XML,
              settingsXmlString
            );
        throw new RuntimeException("Settings not found");
    }

    public DataReturn runForThumbnail() throws IOException {
        return new DataReturn(
                DataReturn.ReturnType.BASE64,
                Base64.getEncoder().encodeToString(thumbnailProviderTS.provideThumbnailData())
        );
    }
}
