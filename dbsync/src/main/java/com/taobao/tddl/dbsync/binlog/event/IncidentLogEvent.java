package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Class representing an incident, an occurance out of the ordinary, that
 * happened on the master. The event is used to inform the slave that something
 * out of the ordinary happened on the master that might cause the database to
 * be in an inconsistent state.
 * <table id="IncidentFormat">
 * <caption>Incident event format</caption>
 * <tr>
 * <th>Symbol</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>INCIDENT</td>
 * <td align="right">2</td>
 * <td>Incident number as an unsigned integer</td>
 * </tr>
 * <tr>
 * <td>MSGLEN</td>
 * <td align="right">1</td>
 * <td>Message length as an unsigned integer</td>
 * </tr>
 * <tr>
 * <td>MESSAGE</td>
 * <td align="right">MSGLEN</td>
 * <td>The message, if present. Not null terminated.</td>
 * </tr>
 * </table>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class IncidentLogEvent extends LogEvent {

    public static final int INCIDENT_NONE        = 0;

    /** There are possibly lost events in the replication stream */
    public static final int INCIDENT_LOST_EVENTS = 1;

    /** Shall be last event of the enumeration */
    public static final int INCIDENT_COUNT       = 2;

    private final int       incident;
    private final String    message;

    public IncidentLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        final int postHeaderLen = descriptionEvent.postHeaderLen[header.type - 1];

        buffer.position(commonHeaderLen);
        final int incidentNumber = buffer.getUint16();
        if (incidentNumber >= INCIDENT_COUNT || incidentNumber <= INCIDENT_NONE) {
            // If the incident is not recognized, this binlog event is
            // invalid. If we set incident_number to INCIDENT_NONE, the
            // invalidity will be detected by is_valid().
            incident = INCIDENT_NONE;
            message = null;
            return;
        }
        incident = incidentNumber;

        buffer.position(commonHeaderLen + postHeaderLen);
        message = buffer.getString();
    }

    public final int getIncident() {
        return incident;
    }

    public final String getMessage() {
        return message;
    }
}
