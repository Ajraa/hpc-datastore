/*******************************************************************************
 * IT4Innovations - National Supercomputing Center
 * Copyright (c) 2017 - 2021 All Right Reserved, https://www.it4i.cz
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this project.
 ******************************************************************************/
package cz.it4i.fiji.datastore;

import static cz.it4i.fiji.datastore.DatasetPathRoutines.getBasePath;
import static cz.it4i.fiji.datastore.DatasetPathRoutines.getDataPath;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javax.ws.rs.NotFoundException;

import org.apache.commons.io.FileUtils;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;

import cz.it4i.fiji.datastore.register_service.Dataset;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;

public class DatasetFilesystemHandler {

	public static final int INITIAL_VERSION = 0;

	private static final Pattern WHOLE_NUMBER_PATTERN = Pattern.compile("\\d+");

	private final Path pathOfDataset;

	private final String uuid;

	public DatasetFilesystemHandler(String auuid, Path path) {
		pathOfDataset = path;
		uuid = auuid;
	}

	public DatasetFilesystemHandler(String auuid, String path) {
		this(auuid, Paths.get(path));
	}

	public DatasetFilesystemHandler(Dataset dataset) {
		this(dataset.getUuid().toString(), dataset.getPath());
	}

	public int createNewVersion() throws IOException {
		int latestVersion = getLatestVersion();
		int newVersion = latestVersion + 1;
		createNewVersion(getBasePath(pathOfDataset, latestVersion), getBasePath(
			pathOfDataset, newVersion));
		return newVersion;
	}

	public Collection<Integer> getAllVersions() throws IOException {
		Collection<Integer> result = new LinkedList<>();
		try (DirectoryStream<Path> ds = Files.newDirectoryStream(pathOfDataset)) {
			for (Path p : (Iterable<Path>) (() -> ds.iterator())) {
				if (!isBlockFileDirOrVersion(p.toFile())) {
					continue;
				}
				Integer temp = Integer.valueOf(p.getFileName().toString());

				result.add(temp);
			}
		}
		return result;
	}

	public N5Writer getWriter() throws IOException {
		return new N5FSWriter(getDataPath(pathOfDataset, getLatestVersion())
			.toString());
	}

	public N5Writer getWriter(String version) throws IOException {
		if (!WHOLE_NUMBER_PATTERN.matcher(version).matches()) {
			return null;
		}
		return getWriter(Integer.parseInt(version));
	}

	public N5Writer getWriter(int versionNumber) throws IOException {
		Path result = getDataPath(pathOfDataset, versionNumber);
		if (!Files.exists(result)) {
			return null;
		}
		return new N5FSWriter(result.toString());
	}

	public int getLatestVersion() throws IOException {
		return Collections.max(getAllVersions());
	}

	public void makeAsInitialVersion(int version) throws IOException {
		Path versionPath = getBasePath(pathOfDataset, version);
		Path initialVersionPath = getBasePath(pathOfDataset, INITIAL_VERSION);
		Files.move(versionPath, initialVersionPath, StandardCopyOption.ATOMIC_MOVE);
	}

	public void deleteVersion(int version) throws IOException {
		Path versionPath = getBasePath(pathOfDataset, version);
		if(!Files.exists(versionPath)) {
			throw new NotFoundException("Dataset with uuid=" + uuid +
				" does not have version " + version);
		}
		// At least one version should remain
		if (getAllVersions().size() == 1) {
			throw new IllegalStateException("Version " + version +
				" is the last version in dataset " + uuid);
		}
		FileUtils.deleteDirectory(versionPath.toFile());
	}

	public  N5Writer constructChainOfWriters() throws IOException {
		return constructChainOfWriters(getLatestVersion());
	}

	public N5Writer constructChainOfWriters(int version) throws IOException {

		N5WriterItemOfChain result = null;
		List<Integer> versions = new LinkedList<>(this.getAllVersions());
		Collections.sort(versions);
		for (Integer i : versions) {
			if (i > version) {
				continue;
			}
			result = new N5WriterItemOfChain(this.getWriter(i),
				result);
		}
		return result;
	}

	private void createNewVersion(Path src, Path dst) throws IOException {
		FileUtils.copyDirectory(src.toFile(), dst.toFile(),
			DatasetFilesystemHandler::isNotBlockFileOrDir);
	}

	private static boolean isBlockFileDirOrVersion(File file) {
		return WHOLE_NUMBER_PATTERN.matcher(file.getName().toString()).matches();
	}

	private static boolean isNotBlockFileOrDir(File file) {
		return !isBlockFileDirOrVersion(file);
	}

	@AllArgsConstructor
	private static class N5WriterItemOfChain implements N5Writer {

		@Delegate(excludes = { ExcludeReadWriteMethod.class })
		private final N5Writer innerWriter;

		private final N5WriterItemOfChain next;

		@Override
		public DataBlock<?> readBlock(String pathName,
			DatasetAttributes datasetAttributes, long[] gridPosition)
			throws IOException
		{
			DataBlock<?> result = innerWriter.readBlock(pathName, datasetAttributes,
				gridPosition);
			if (result != null) {
				return result;
			}

			if (next != null) {
				return next.readBlock(pathName, datasetAttributes, gridPosition);
			}
			return null;
		}

		@Override
		public <T> void writeBlock(String pathName,
			DatasetAttributes datasetAttributes, DataBlock<T> dataBlock)
			throws IOException
		{
			throw new UnsupportedOperationException(
				"Writting mode is not supported for version mixedLatest");
		}

	}

	private interface ExcludeReadWriteMethod {

		public DataBlock<?> readBlock(final String pathName,
			final DatasetAttributes datasetAttributes, final long[] gridPosition)
			throws IOException;

		public <T> void writeBlock(final String pathName,
			final DatasetAttributes datasetAttributes, final DataBlock<T> dataBlock)
			throws IOException;

	}
}
