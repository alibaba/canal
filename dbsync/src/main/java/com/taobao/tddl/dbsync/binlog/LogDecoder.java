package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.taobao.tddl.dbsync.binlog.event.AppendBlockLogEvent;
import com.taobao.tddl.dbsync.binlog.event.BeginLoadQueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.CreateFileLogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteFileLogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.ExecuteLoadLogEvent;
import com.taobao.tddl.dbsync.binlog.event.ExecuteLoadQueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.GtidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.HeartbeatLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IgnorableLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IncidentLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IntvarLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LoadLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;
import com.taobao.tddl.dbsync.binlog.event.PreviousGtidsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RandLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsQueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.StartLogEventV3;
import com.taobao.tddl.dbsync.binlog.event.StopLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TransactionContextLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UnknownLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UserVarLogEvent;
import com.taobao.tddl.dbsync.binlog.event.ViewChangeEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XaPrepareLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.AnnotateRowsEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.BinlogCheckPointLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.MariaGtidListLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.MariaGtidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.StartEncryptionLogEvent;

/**
 * Implements a binary-log decoder.
 *
 * <pre>
 * LogDecoder decoder = new LogDecoder();
 * decoder.handle(...);
 * 
 * LogEvent event;
 * do
 * {
 *     event = decoder.decode(buffer, context);
 * 
 *     // process log event.
 * }
 * while (event != null);
 * // no more events in buffer.
 * </pre>
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class LogDecoder {

    protected static final Log logger    = LogFactory.getLog(LogDecoder.class);

    protected final BitSet     handleSet = new BitSet(LogEvent.ENUM_END_EVENT);

    public LogDecoder(){
    }

    public LogDecoder(final int fromIndex, final int toIndex){
        handleSet.set(fromIndex, toIndex);
    }

    public final void handle(final int fromIndex, final int toIndex) {
        handleSet.set(fromIndex, toIndex);
    }

    public final void handle(final int flagIndex) {
        handleSet.set(flagIndex);
    }

    /**
     * Decoding an event from binary-log buffer.
     *
     * @return <code>UknownLogEvent</code> if event type is unknown or skipped,
     * <code>null</code> if buffer is not including a full event.
     */
    public LogEvent decode(LogBuffer buffer, LogContext context) throws IOException {
        final int limit = buffer.limit();

        if (limit >= FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN) {
            LogHeader header = new LogHeader(buffer, context.getFormatDescription());

            final int len = header.getEventLen();
            if (limit >= len) {
                LogEvent event;

                /* Checking binary-log's header */
                if (handleSet.get(header.getType())) {
                    buffer.limit(len);
                    try {
                        /* Decoding binary-log to event */
                        event = decode(buffer, header, context);
                    } catch (IOException e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Decoding " + LogEvent.getTypeName(header.getType()) + " failed from: "
                                        + context.getLogPosition(), e);
                        }
                        throw e;
                    } finally {
                        buffer.limit(limit); /* Restore limit */
                    }
                } else {
                    /* Ignore unsupported binary-log. */
                    event = new UnknownLogEvent(header);
                }

                if (event != null) {
                    // set logFileName
                    event.getHeader().setLogFileName(context.getLogPosition().getFileName());
                    event.setSemival(buffer.semival);
                }

                /* consume this binary-log. */
                buffer.consume(len);
                return event;
            }
        }

        /* Rewind buffer's position to 0. */
        buffer.rewind();
        return null;
    }

    /**
     * Deserialize an event from buffer.
     *
     * @return <code>UknownLogEvent</code> if event type is unknown or skipped.
     */
    public static LogEvent decode(LogBuffer buffer, LogHeader header, LogContext context) throws IOException {
        FormatDescriptionLogEvent descriptionEvent = context.getFormatDescription();
        LogPosition logPosition = context.getLogPosition();

        int checksumAlg = LogEvent.BINLOG_CHECKSUM_ALG_UNDEF;
        if (header.getType() != LogEvent.FORMAT_DESCRIPTION_EVENT) {
            checksumAlg = descriptionEvent.header.getChecksumAlg();
        } else {
            // 如果是format事件自己，也需要处理checksum
            checksumAlg = header.getChecksumAlg();
        }

        if (checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_OFF && checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_UNDEF) {
            // remove checksum bytes
            buffer.limit(header.getEventLen() - LogEvent.BINLOG_CHECKSUM_LEN);
        }
        GTIDSet gtidSet = context.getGtidSet();
        GtidLogEvent gtidLogEvent = context.getGtidLogEvent();
        switch (header.getType()) {
            case LogEvent.QUERY_EVENT: {
                QueryLogEvent event = new QueryLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.XID_EVENT: {
                XidLogEvent event = new XidLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.TABLE_MAP_EVENT: {
                TableMapLogEvent mapEvent = new TableMapLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                context.putTable(mapEvent);
                return mapEvent;
            }
            case LogEvent.WRITE_ROWS_EVENT_V1: {
                RowsLogEvent event = new WriteRowsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.UPDATE_ROWS_EVENT_V1: {
                RowsLogEvent event = new UpdateRowsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.DELETE_ROWS_EVENT_V1: {
                RowsLogEvent event = new DeleteRowsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.ROTATE_EVENT: {
                RotateLogEvent event = new RotateLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition = new LogPosition(event.getFilename(), event.getPosition());
                context.setLogPosition(logPosition);
                return event;
            }
            case LogEvent.LOAD_EVENT:
            case LogEvent.NEW_LOAD_EVENT: {
                LoadLogEvent event = new LoadLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.SLAVE_EVENT: /* can never happen (unused event) */
            {
                if (logger.isWarnEnabled()) logger.warn("Skipping unsupported SLAVE_EVENT from: "
                                                        + context.getLogPosition());
                break;
            }
            case LogEvent.CREATE_FILE_EVENT: {
                CreateFileLogEvent event = new CreateFileLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.APPEND_BLOCK_EVENT: {
                AppendBlockLogEvent event = new AppendBlockLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.DELETE_FILE_EVENT: {
                DeleteFileLogEvent event = new DeleteFileLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.EXEC_LOAD_EVENT: {
                ExecuteLoadLogEvent event = new ExecuteLoadLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.START_EVENT_V3: {
                /* This is sent only by MySQL <=4.x */
                StartLogEventV3 event = new StartLogEventV3(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.STOP_EVENT: {
                StopLogEvent event = new StopLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.INTVAR_EVENT: {
                IntvarLogEvent event = new IntvarLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.RAND_EVENT: {
                RandLogEvent event = new RandLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.USER_VAR_EVENT: {
                UserVarLogEvent event = new UserVarLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.FORMAT_DESCRIPTION_EVENT: {
                descriptionEvent = new FormatDescriptionLogEvent(header, buffer, descriptionEvent);
                context.setFormatDescription(descriptionEvent);
                return descriptionEvent;
            }
            case LogEvent.PRE_GA_WRITE_ROWS_EVENT: {
                if (logger.isWarnEnabled()) logger.warn("Skipping unsupported PRE_GA_WRITE_ROWS_EVENT from: "
                                                        + context.getLogPosition());
                // ev = new Write_rows_log_event_old(buf, event_len,
                // description_event);
                break;
            }
            case LogEvent.PRE_GA_UPDATE_ROWS_EVENT: {
                if (logger.isWarnEnabled()) logger.warn("Skipping unsupported PRE_GA_UPDATE_ROWS_EVENT from: "
                                                        + context.getLogPosition());
                // ev = new Update_rows_log_event_old(buf, event_len,
                // description_event);
                break;
            }
            case LogEvent.PRE_GA_DELETE_ROWS_EVENT: {
                if (logger.isWarnEnabled()) logger.warn("Skipping unsupported PRE_GA_DELETE_ROWS_EVENT from: "
                                                        + context.getLogPosition());
                // ev = new Delete_rows_log_event_old(buf, event_len,
                // description_event);
                break;
            }
            case LogEvent.BEGIN_LOAD_QUERY_EVENT: {
                BeginLoadQueryLogEvent event = new BeginLoadQueryLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.EXECUTE_LOAD_QUERY_EVENT: {
                ExecuteLoadQueryLogEvent event = new ExecuteLoadQueryLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.INCIDENT_EVENT: {
                IncidentLogEvent event = new IncidentLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.HEARTBEAT_LOG_EVENT: {
                HeartbeatLogEvent event = new HeartbeatLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.IGNORABLE_LOG_EVENT: {
                IgnorableLogEvent event = new IgnorableLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.ROWS_QUERY_LOG_EVENT: {
                RowsQueryLogEvent event = new RowsQueryLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.WRITE_ROWS_EVENT: {
                RowsLogEvent event = new WriteRowsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.UPDATE_ROWS_EVENT: {
                RowsLogEvent event = new UpdateRowsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.DELETE_ROWS_EVENT: {
                RowsLogEvent event = new DeleteRowsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.PARTIAL_UPDATE_ROWS_EVENT: {
                RowsLogEvent event = new UpdateRowsLogEvent(header, buffer, descriptionEvent, true);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                event.fillTable(context);
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.GTID_LOG_EVENT:
            case LogEvent.ANONYMOUS_GTID_LOG_EVENT: {
                GtidLogEvent event = new GtidLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                if (gtidSet != null) {
                    gtidSet.update(event.getGtidStr());
                    // update latest gtid
                    header.putGtid(gtidSet, event);
                }
                // update current gtid event to context
                context.setGtidLogEvent(event);
                return event;
            }
            case LogEvent.PREVIOUS_GTIDS_LOG_EVENT: {
                PreviousGtidsLogEvent event = new PreviousGtidsLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.TRANSACTION_CONTEXT_EVENT: {
                TransactionContextLogEvent event = new TransactionContextLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.VIEW_CHANGE_EVENT: {
                ViewChangeEvent event = new ViewChangeEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.XA_PREPARE_LOG_EVENT: {
                XaPrepareLogEvent event = new XaPrepareLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.ANNOTATE_ROWS_EVENT: {
                AnnotateRowsEvent event = new AnnotateRowsEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                header.putGtid(context.getGtidSet(), gtidLogEvent);
                return event;
            }
            case LogEvent.BINLOG_CHECKPOINT_EVENT: {
                BinlogCheckPointLogEvent event = new BinlogCheckPointLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.GTID_EVENT: {
                MariaGtidLogEvent event = new MariaGtidLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.GTID_LIST_EVENT: {
                MariaGtidListLogEvent event = new MariaGtidListLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            case LogEvent.START_ENCRYPTION_EVENT: {
                StartEncryptionLogEvent event = new StartEncryptionLogEvent(header, buffer, descriptionEvent);
                /* updating position in context */
                logPosition.position = header.getLogPos();
                return event;
            }
            default:
                /*
                 * Create an object of Ignorable_log_event for unrecognized
                 * sub-class. So that SLAVE SQL THREAD will only update the
                 * position and continue.
                 */
                if ((buffer.getUint16(LogEvent.FLAGS_OFFSET) & LogEvent.LOG_EVENT_IGNORABLE_F) > 0) {
                    IgnorableLogEvent event = new IgnorableLogEvent(header, buffer, descriptionEvent);
                    /* updating position in context */
                    logPosition.position = header.getLogPos();
                    return event;
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skipping unrecognized binlog event " + LogEvent.getTypeName(header.getType())
                                    + " from: " + context.getLogPosition());
                    }
                }
        }

        /* updating position in context */
        logPosition.position = header.getLogPos();
        /* Unknown or unsupported log event */
        return new UnknownLogEvent(header);
    }
}
