package com.alibaba.otter.canal.parse.inbound.mongodb.dbsync;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.util.List;

/**
 * Mongodb  ChangeStreamEvent
 *
 * @author  jiabao.sun 2020-07-13 15:23:21
 * @version 1.0.0
 */
public class ChangeStreamEvent {

    public static final String LOG_FILE_COLLECTION = "oplog.rs";

    private Document resumeToken;
    private MongoNamespace namespace;
    private MongoNamespace destinationNamespace;
    private Document fullDocument;
    private Document documentKey;
    private BsonTimestamp clusterTime;
    private OperationType operationType;
    private UpdateDescription updateDescription;
    private long txnNumber;
    private Document lsid;

    public static ChangeStreamEvent from(ChangeStreamDocument<Document> changeStreamDoc) {
        if (changeStreamDoc == null) {
            return null;
        }

        ChangeStreamEvent changeStreamEvent = new ChangeStreamEvent();
        changeStreamEvent.setResumeToken(BsonConverter.convert(changeStreamDoc.getResumeToken()));
        changeStreamEvent.setNamespace(changeStreamDoc.getNamespace());
        changeStreamEvent.setDestinationNamespace(changeStreamDoc.getDestinationNamespace());
        changeStreamEvent.setFullDocument(changeStreamDoc.getFullDocument());
        changeStreamEvent.setDocumentKey(BsonConverter.convert(changeStreamDoc.getDocumentKey()));
        changeStreamEvent.setClusterTime(changeStreamDoc.getClusterTime());
        changeStreamEvent.setOperationType(changeStreamDoc.getOperationType());

        if (changeStreamDoc.getUpdateDescription() != null) {
            ChangeStreamEvent.UpdateDescription updateDescription = new ChangeStreamEvent.UpdateDescription();
            updateDescription.setRemovedFields(changeStreamDoc.getUpdateDescription().getRemovedFields());
            if (changeStreamDoc.getUpdateDescription().getUpdatedFields() != null) {
                updateDescription.setUpdatedFields(
                        BsonConverter.convert(changeStreamDoc.getUpdateDescription().getUpdatedFields()));
            }
            changeStreamEvent.setUpdateDescription(updateDescription);
        }

        if (changeStreamDoc.getTxnNumber() != null) {
            changeStreamEvent.setTxnNumber(changeStreamDoc.getTxnNumber().longValue());
        }

        changeStreamEvent.setLsid(BsonConverter.convert(changeStreamDoc.getLsid()));

        return changeStreamEvent;
    }

    public static class UpdateDescription {
        private List<String> removedFields;
        private Document updatedFields;

        public List<String> getRemovedFields() {
            return removedFields;
        }

        public void setRemovedFields(List<String> removedFields) {
            this.removedFields = removedFields;
        }

        public Document getUpdatedFields() {
            return updatedFields;
        }

        public void setUpdatedFields(Document updatedFields) {
            this.updatedFields = updatedFields;
        }
    }

    public Document getResumeToken() {
        return resumeToken;
    }

    public void setResumeToken(Document resumeToken) {
        this.resumeToken = resumeToken;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public void setNamespace(MongoNamespace namespace) {
        this.namespace = namespace;
    }

    public MongoNamespace getDestinationNamespace() {
        return destinationNamespace;
    }

    public void setDestinationNamespace(MongoNamespace destinationNamespace) {
        this.destinationNamespace = destinationNamespace;
    }

    public Document getFullDocument() {
        return fullDocument;
    }

    public void setFullDocument(Document fullDocument) {
        this.fullDocument = fullDocument;
    }

    public Document getDocumentKey() {
        return documentKey;
    }

    public void setDocumentKey(Document documentKey) {
        this.documentKey = documentKey;
    }

    public BsonTimestamp getClusterTime() {
        return clusterTime;
    }

    public void setClusterTime(BsonTimestamp clusterTime) {
        this.clusterTime = clusterTime;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public UpdateDescription getUpdateDescription() {
        return updateDescription;
    }

    public void setUpdateDescription(UpdateDescription updateDescription) {
        this.updateDescription = updateDescription;
    }

    public long getTxnNumber() {
        return txnNumber;
    }

    public void setTxnNumber(long txnNumber) {
        this.txnNumber = txnNumber;
    }

    public Document getLsid() {
        return lsid;
    }

    public void setLsid(Document lsid) {
        this.lsid = lsid;
    }
}
