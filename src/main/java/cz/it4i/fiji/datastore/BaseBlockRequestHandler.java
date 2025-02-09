package cz.it4i.fiji.datastore;

import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseBlockRequestHandler<T> {
    private static final Pattern URL_BLOCKS_PATTERN = Pattern.compile(
            "(\\p{Digit}+)/(\\p{Digit}+)/(\\p{Digit}+)/(\\p{Digit}+)/(\\p{Digit}+)/(\\p{Digit}+)");

    public abstract T readBlock(DatasetServerImpl datasetServer, long x, long y,
                              long z, int time, int channel, int angle, String blocks) throws IOException;

    public abstract T writeBlock(DatasetServerImpl datasetServer, long x, long y,
                               long z, int time, int channel, int angle, String blocks,
                               InputStream inputStream) throws IOException;

    public abstract T getType(DatasetServerImpl datasetServer, int time,
                            int channel, int angle);

    @Getter
    @AllArgsConstructor
    protected static class BlockIdentification {

        protected final long[] gridPosition;

        protected final int time;

        protected final int channel;

        protected final int angle;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (long i : gridPosition) {
                sb.append(i).append("/");
            }
            sb.append(time).append("/").append(channel).append("/").append(angle);
            return sb.toString();
        }

        static void extract(String blocks,
                            List<BlockIdentification> blocksId)
        {

            Matcher matcher = URL_BLOCKS_PATTERN.matcher(blocks);
            while (matcher.find()) {
                blocksId.add(new BlockIdentification(new long[] { getLong(matcher, 1),
                        getLong(matcher, 2), getLong(matcher, 3) }, getInt(matcher, 4),
                        getInt(matcher, 5), getInt(matcher, 6)));
            }
        }

        static int getInt(Matcher matcher, int i) {
            return Integer.parseInt(matcher.group(i));
        }

        static long getLong(Matcher matcher, int i) {
            return Long.parseLong(matcher.group(i));
        }
    }
}
