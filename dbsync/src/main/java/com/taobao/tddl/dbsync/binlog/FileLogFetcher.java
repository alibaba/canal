package com.taobao.tddl.dbsync.binlog;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;

/**
 * TODO: Document It!!
 * 
 * <pre>
 * FileLogFetcher fetcher = new FileLogFetcher();
 * fetcher.open(file, 0);
 * 
 * while (fetcher.fetch()) {
 *     LogEvent event;
 *     do {
 *         event = decoder.decode(fetcher, context);
 * 
 *         // process log event.
 *     } while (event != null);
 * }
 * // file ending reached.
 * </pre>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class FileLogFetcher extends LogFetcher {

    public static final byte[] BINLOG_MAGIC = { -2, 0x62, 0x69, 0x6e };

    private FileInputStream    fin;

    public FileLogFetcher(){
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public FileLogFetcher(final int initialCapacity){
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public FileLogFetcher(final int initialCapacity, final float growthFactor){
        super(initialCapacity, growthFactor);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(File file) throws FileNotFoundException, IOException {
        open(file, 0L);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(String filePath) throws FileNotFoundException, IOException {
        open(new File(filePath), 0L);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(String filePath, final long filePosition) throws FileNotFoundException, IOException {
        open(new File(filePath), filePosition);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(File file, final long filePosition) throws FileNotFoundException, IOException {
        fin = new FileInputStream(file);

        ensureCapacity(BIN_LOG_HEADER_SIZE);
        if (BIN_LOG_HEADER_SIZE != fin.read(buffer, 0, BIN_LOG_HEADER_SIZE)) {
            throw new IOException("No binlog file header");
        }

        if (buffer[0] != BINLOG_MAGIC[0] || buffer[1] != BINLOG_MAGIC[1] || buffer[2] != BINLOG_MAGIC[2]
            || buffer[3] != BINLOG_MAGIC[3]) {
            throw new IOException("Error binlog file header: "
                                  + Arrays.toString(Arrays.copyOf(buffer, BIN_LOG_HEADER_SIZE)));
        }

        limit = 0;
        origin = 0;
        position = 0;

        if (filePosition > BIN_LOG_HEADER_SIZE) {
            final int maxFormatDescriptionEventLen = FormatDescriptionLogEvent.LOG_EVENT_MINIMAL_HEADER_LEN
                                                     + FormatDescriptionLogEvent.ST_COMMON_HEADER_LEN_OFFSET
                                                     + LogEvent.ENUM_END_EVENT + LogEvent.BINLOG_CHECKSUM_ALG_DESC_LEN
                                                     + LogEvent.CHECKSUM_CRC32_SIGNATURE_LEN;

            ensureCapacity(maxFormatDescriptionEventLen);
            limit = fin.read(buffer, 0, maxFormatDescriptionEventLen);
            limit = (int) getUint32(LogEvent.EVENT_LEN_OFFSET);
            fin.getChannel().position(filePosition);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#fetch()
     */
    public boolean fetch() throws IOException {
        if (limit == 0) {
            final int len = fin.read(buffer, 0, buffer.length);
            if (len >= 0) {
                limit += len;
                position = 0;
                origin = 0;

                /* More binlog to fetch */
                return true;
            }
        } else if (origin == 0) {
            if (limit > buffer.length / 2) {
                ensureCapacity(buffer.length + limit);
            }
            final int len = fin.read(buffer, limit, buffer.length - limit);
            if (len >= 0) {
                limit += len;

                /* More binlog to fetch */
                return true;
            }
        } else if (limit > 0) {
            if (limit >= FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN) {
                int lenPosition = position + 4 + 1 + 4;
                long eventLen = ((long) (0xff & buffer[lenPosition++])) | ((long) (0xff & buffer[lenPosition++]) << 8)
                                | ((long) (0xff & buffer[lenPosition++]) << 16)
                                | ((long) (0xff & buffer[lenPosition++]) << 24);

                if (limit >= eventLen) {
                    return true;
                } else {
                    ensureCapacity((int) eventLen);
                }
            }

            System.arraycopy(buffer, origin, buffer, 0, limit);
            position -= origin;
            origin = 0;
            final int len = fin.read(buffer, limit, buffer.length - limit);
            if (len >= 0) {
                limit += len;

                /* More binlog to fetch */
                return true;
            }
        } else {
            /* Should not happen. */
            throw new IllegalArgumentException("Unexcepted limit: " + limit);
        }

        /* Reach binlog file end */
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#close()
     */
    public void close() throws IOException {
        if (fin != null) {
            fin.close();
        }

        fin = null;
    }
}
