import request = require('request');
import kafka = require('kafka-node');
import sentry = require('raven');
import {DB_PASSWORD, DB_URL, DB_USER, KAFKA_HOST, SENTRY_DNS} from "./config";

let consumerGroup: kafka.ConsumerGroup;

sentry.config(SENTRY_DNS).install();

function customLog(msg: string) {
    console.log((+ new Date()) + ' - ' + msg);
}

if (process.argv.length === 3) {
    let typeArg = process.argv[2];
    const apidateType = typeArg.split('=')[1];
    customLog('Creating consumer group for topic ' + apidateType);
    consumerGroup = new kafka.ConsumerGroup(
        {kafkaHost: KAFKA_HOST, groupId: 'apidate-consumer', fromOffset: 'latest', autoCommit: true},
        apidateType
    );
    customLog('consumer group created');
    startProcessing(apidateType);
} else {
    customLog('unsupported args: ' + process.argv.join(' '));
    process.exit(1);
}

function startProcessing(apidateType: string) {
    customLog('startProcessing topic ' + apidateType);
    consumerGroup.on('error', (error: Error) => {
        customLog('consumer error: ' + error.message);
        sentry.captureMessage('consumer error: ' + error.message);
    });
    consumerGroup.on('offsetOutOfRange', (error: Error) => {
        customLog('consumer offsetOutOfRange: ' + error.message);
        sentry.captureMessage('consumer offsetOutOfRange: ' + error.message);
    });
    consumerGroup.on('message', (message: kafka.Message) => {
        try {
            let payload = JSON.parse(message.value as string);
            if (payload.operation === 'ADD_PERIOD') {
                createDocument(apidateType, payload);
            } else if (payload.operation === 'DUPLICATE_PERIOD') {
                cloneDocument(apidateType, payload);
            } else if (payload.operation === 'DELETE_PERIOD') {
                deleteDocument(apidateType, payload);
            } else if (payload.operation === 'UPDATE_PERIOD') {
                updateDocument(apidateType, payload);
            } else {
                customLog('unsupported operation: ' + payload.operation);
            }
        } catch (e) {
            customLog('invalid message value: ' + message.value);
            sentry.captureMessage('invalid message value: ' + message.value);
        }
    });
}

function createDocument(apidateType: string, payload: any) {
    const typeData = {type: apidateType};
    request.post(DB_URL, {
        headers: {'content-type': 'application/json'},
        auth: {user: DB_USER, password: DB_PASSWORD},
        json: {...payload.payload, ...typeData},
    }, (err: any, resp: any, bdy: any) => {
        if (!err && resp.statusCode === 201 && bdy.ok) {
            customLog('created new doc ' + payload.sourceId + ': ' + bdy.id);
        } else {
            customLog('failed to create doc ' + payload.sourceId + ': ' +
                resp.statusCode + ' - ' + (err ? err.message : 'unknown error'));
        }
    });
}

function cloneDocument(apidateType: string, payload: any) {
    request.get(`${DB_URL}/_design/api/_list/details/by-external-id?key="${payload.sourceObjectId}"&type=${apidateType}`,
        {auth: {user: DB_USER, password: DB_PASSWORD}},
        (error, response, body) => {
            if (!error && response.statusCode === 200) {
                let clonedDoc = JSON.parse(body);
                clonedDoc.externalId = payload.duplicatedObjectId;
                clonedDoc.userId = payload.userId;
                clonedDoc.updatedAt = new Date().getTime();
                request.post(DB_URL, {
                    headers: {'content-type': 'application/json'},
                    auth: {user: DB_USER, password: DB_PASSWORD},
                    json: clonedDoc
                }, (err: any, resp: any, bdy: any) => {
                    if (!err && resp.statusCode === 201 && bdy.ok) {
                        customLog('created duplicate doc ' + payload.duplicatedObjectId + ': ' + bdy.id);
                    } else {
                        customLog('failed to duplicate doc ' + payload.sourceObjectId + ': ' +
                            resp.statusCode + ' - ' + (err ? err.message : 'unknown error'));
                    }
                });
            }
            // else {
            //     customLog('failed to retrieve doc ' + payload.sourceObjectId + ': ' +
            //         response.statusCode + ' - ' + (error ? error.message : 'unknown error'));
            // }
        });
}

function deleteDocument(apidateType: string, payload: any) {
    request.get(`${DB_URL}/_design/api/_list/ids/by-type-and-external-id?key=["${apidateType}","${payload.periodId}"]`,
        {auth: {user: DB_USER, password: DB_PASSWORD}},
        (error, response, body) => {
            if (!error && response.statusCode === 200) {
                let docsToDelete = JSON.parse(body);
                for (let doc of docsToDelete) {
                    request.delete(`${DB_URL}/${doc.id}?rev=${doc.rev}`, {auth: {user: DB_USER, password: DB_PASSWORD}},
                        (err, resp, bdy) => {
                            let respBody = JSON.parse(bdy);
                            if (!err && resp.statusCode === 200 && respBody.ok) {
                                customLog('deleted doc ' + payload.periodId + ': ' + respBody.id + ' | ' + respBody.rev);
                            } else {
                                customLog('failed to delete doc ' + payload.periodId + ': ' +
                                    resp.statusCode + ' - ' + (err ? err.message : 'unknown error'));
                            }
                        });
                }
            }
            // else {
            //     customLog('failed to retrieve doc ' + payload.periodId + ': ' +
            //         response.statusCode + ' - ' + (error ? error.message : 'unknown error'));
            // }
        });
}

function updateDocument(apidateType: string, payload: any) {
    request.get(`${DB_URL}/_design/api/_view/by-type-and-external-id?key=["${apidateType}","${payload.periodId}"]`,
        {auth: {user: DB_USER, password: DB_PASSWORD}},
        (error, response, body) => {
            if (!error && response.statusCode === 200) {
                let resp_body = JSON.parse(body);
                if (resp_body.rows && resp_body.rows.length === 1) {
                    let docToUpdate = resp_body.rows[0].value;
                    request.put(`${DB_URL}/${docToUpdate._id}?rev=${docToUpdate._rev}`, {
                        headers: {'content-type': 'application/json'},
                        auth: {user: DB_USER, password: DB_PASSWORD},
                        json: {...docToUpdate, ...payload.updatedObject}
                    }, (err, resp, bdy) => {
                        if (!err && resp.statusCode === 201 && bdy.ok) {
                            customLog('updated doc ' + payload.periodId + ': ' + bdy.id + ' | ' + bdy.rev);
                        } else {
                            customLog('failed to update doc ' + payload.periodId + ': ' +
                                resp.statusCode + ' - ' + (err ? err.message : 'unknown error'));
                        }
                    });
                }

            }
        }
    );
}
