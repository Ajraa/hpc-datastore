/*******************************************************************************
 * IT4Innovations - National Supercomputing Center
 * Copyright (c) 2017 - 2020 All Right Reserved, https://www.it4i.cz
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this project.
 ******************************************************************************/
package cz.it4i.fiji.datastore.register_service;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import cz.it4i.fiji.datastore.bdv_server.CellHandlerTSProducer;


@RequestScoped
public class DatasetRegisterServiceTSImpl extends DatasetRegisterServiceImplBase<URI> {

	@Inject
	CellHandlerTSProducer writeToVersionListener;

	@Override
	public URI start(String uuid, int[] r, String version, OperationMode mode,
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
			resolvedVersion, version.equals("mixedLatest"), mode, timeout).getUri();
	}

	@Override
	public URI start(String uuid, List<int[]> resolutions, Long timeout)
		throws IOException
	{
		Dataset dataset = getDataset(uuid);
		// called only for checking that all resolutions exists
		getNonIdentityResolutions(dataset, resolutions);
		mergeVersions(dataset);
		writeToVersionListener.writeToAllVersions(uuid);
		return dataServerManager.startDataServer(dataset.getUuid(), resolutions,
			timeout).getUri();
	}

	/*private void checkeResolutions(List<ResolutionLevel> levels) {
		int[] previousResolution = null;
		for (ResolutionLevel level : levels) {
			int[] resolution = level.getResolutions();
			if (previousResolution == null) {
				previousResolution = resolution;
				continue;
			}
			for (int dim = 0; dim < previousResolution.length; dim++) {
				if (resolution[dim] % previousResolution[dim] != 0) {
					throw new UnsupportedOperationException(String.format(
						"Cannot rescale from resolution %s to resolution %s", toString(
							previousResolution), toString(resolution)));
				}
			}
			previousResolution= resolution;
		}
	
	}*/

}
