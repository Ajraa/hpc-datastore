package cz.it4i.fiji.datastore;

import cz.it4i.fiji.datastore.bdv_server.DataReturn;
import lombok.extern.log4j.Log4j2;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

@Log4j2
@Default
@ApplicationScoped
public class GQLBlockRequestHandler extends BaseBlockRequestHandler<DataReturn> {

    @Override
    public DataReturn readBlock(DatasetServerImpl datasetServer, long x, long y, long z, int time, int channel, int angle, String blocks) throws IOException {
        try {
            List<BlockIdentification> blocksId = new LinkedList<>();
            blocksId.add(new BlockIdentification(new long[] { x, y, z }, time,
                    channel, angle));
            BlockIdentification.extract(blocks, blocksId);
            DataType dataType = null;
            try (DataBlockInputStream result = new DataBlockInputStream()) {
                for (BlockIdentification bi : blocksId) {
                    long[] position = new long[] { bi.gridPosition[0], bi.gridPosition[1],
                            bi.gridPosition[2] };
                    DataBlock<?> block = datasetServer.read(position, bi.time, bi.channel,
                            bi.angle);
                    // block do not exist - return empty block having size [-1, -1, -1]
                    if (block == null) {
                        if (dataType == null) {
                            dataType = datasetServer.getType(time, channel, angle);
                        }
                        block = dataType.createDataBlock(new int[] { -1, -1, -1 }, position,
                                0);
                    }
                    result.add(block);
                }
                return new DataReturn(
                        DataReturn.ReturnType.BASE64,
                        Base64.getEncoder().encodeToString(result.readAllBytes())
                );
            }
        }
        catch (IOException | NullPointerException exc) {
            log.warn("read", exc);
            throw exc;
        }
    }

    @Override
    public DataReturn writeBlock(DatasetServerImpl datasetServer, long x, long y, long z, int time, int channel, int angle, String blocks, InputStream inputStream) throws IOException {
        List<BlockIdentification> blocksId = new LinkedList<>();
        blocksId.add(new BlockIdentification(new long[] { x, y, z }, time, channel,
                angle));
        BlockIdentification.extract(blocks, blocksId);
        try {

            for (BlockIdentification blockId : blocksId) {
                datasetServer.write(blockId.gridPosition, blockId.time, blockId.channel,
                        blockId.angle, inputStream);
            }
        }
        catch (IOException exc) {
            log.warn("write", exc);
            throw exc;
        }
        return new DataReturn(
                DataReturn.ReturnType.SUCCESS,
                null
        );
    }

    @Override
    public DataReturn getType(DatasetServerImpl datasetServer, int time, int channel, int angle) {
        DataType dt = datasetServer.getType(time, channel, angle);
        if (dt != null) {
            return new DataReturn(
                    DataReturn.ReturnType.TEXT,
                    dt.toString()
            );
        }
        return null;
    }
}
