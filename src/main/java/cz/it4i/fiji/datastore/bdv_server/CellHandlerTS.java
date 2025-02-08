package cz.it4i.fiji.datastore.bdv_server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import lombok.extern.log4j.Log4j2;
import net.imglib2.img.cell.Cell;
import org.janelia.saalfeldlab.n5.DataBlock;
import bdv.spimdata.XmlIoSpimDataMinimal;
import cz.it4i.fiji.datastore.DatasetHandler;
import cz.it4i.fiji.datastore.core.HPCDatastoreImageLoaderMetaData;
import cz.it4i.fiji.datastore.register_service.Dataset;
import mpicbg.spim.data.SpimDataException;

@Log4j2
public class CellHandlerTS  extends CellHandlerBase<Response>
{


	CellHandlerTS(DatasetHandler datasetHandler, Supplier<Dataset> datasetSupplier, String baseUrl, int version, String datasetName, String thumbnailsDirectory) throws IOException {
		super(datasetHandler, datasetSupplier, baseUrl, version, datasetName, thumbnailsDirectory);
	}



	@SuppressWarnings("unchecked")
	public Response runForCellOrInit(final String cellString) {
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
	
			return Response.ok(new ByteArrayInputStream(data)).type(
				MediaType.APPLICATION_OCTET_STREAM_TYPE).build();

		}
		else if (parts[0].equals("init"))
		{
			HPCDatastoreImageLoaderMetaData[] metadataArray = { null };
			Response retVal = respondWithString("application/json", buildMetadataJsonString(
					spimdataSupplier.get(), datasetSupplier.get(), metadataArray));
			this.metadata = metadataArray[0];
			return retVal;
		}
		return Response.status(Status.BAD_REQUEST).build();
	}

	@Override
	public Response runForDataset() {
		final XmlIoSpimDataMinimal io = new XmlIoSpimDataMinimal();

		try {
			return respondWithString("application/xml", buildRemoteDatasetXML(io,
				spimdataSupplier.get(), baseUrl));
		}
		catch (IOException | SpimDataException exc) {
			throw new RuntimeException(exc);
		}
	}

	public Response runForSettings() {
		if (settingsXmlString != null) {
			return respondWithString("application/xml", settingsXmlString);
		}
		return Response.status(Status.NOT_FOUND).entity("settings.xml").build();
	}

	public void runForThumbnail(final HttpServletResponse response)
		throws IOException
	{
		thumbnailProviderTS.runForThumbnail(response);
	}

	/**
	 * Handle request by sending a UTF-8 string.
	 */
	private static Response respondWithString(final String contentType,
		final String string)
	{
		return Response.ok(string).type(contentType).encoding("UTF-8").build();
	}
}
