import axios, {AxiosInstance} from 'axios';
import kafka = require('kafka-node');
import sentry = require('raven');
import async = require('async');
import {AsyncQueue} from "async";
import {DB_PASSWORD, DB_URL, DB_USER, KAFKA_HOST, SENTRY_DNS} from "./config";

let consumerGroup: kafka.ConsumerGroup;
let processingQueue: AsyncQueue<kafka.Message>;
let httpClient: AxiosInstance;

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

    httpClient = axios.create({
        withCredentials: true,
        auth: {username: DB_USER, password: DB_PASSWORD},
        headers: {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    });
    customLog('http client created');

    processingQueue = async.queue(function(message, processingCallback) {
        processMessage(apidateType, message).then(function(processingResult) {
            processingCallback();
        });
    }, 1);

    processingQueue.drain = function() {
        consumerGroup.resume();
    };

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
        processingQueue.push(message, function (err, result) {
            if (err) {
                customLog('message handling error: ' + err.message);
                sentry.captureMessage('message handling error: ' + err.message);
            }
        });
        consumerGroup.pause();
    });
}

function processMessage(apidateType: string, message: kafka.Message) {
    let result;
    try {
        let payload = JSON.parse(message.value as string);
        customLog('handling message with operation ' + payload.operation + ' and sourceId ' + payload.sourceId);
        if (payload.operation === 'ADD_PERIOD') {
            result = createDocument(apidateType, payload);
        } else if (payload.operation === 'DUPLICATE_PERIOD') {
            result = cloneDocument(apidateType, payload);
        } else if (payload.operation === 'DELETE_PERIOD') {
            result = deleteDocument(apidateType, payload);
        } else if (payload.operation === 'UPDATE_PERIOD') {
            result = updateDocument(apidateType, payload);
        } else {
            customLog('unsupported operation: ' + payload.operation);
            result = Promise.reject('unsupported operation: ' + payload.operation);
        }
    } catch (e) {
        const msg = 'invalid message value: ' + message.value;
        customLog(msg);
        sentry.captureMessage(msg);
        result = Promise.reject(msg);
    }
    return result;
}

function createDocument(apidateType: string, payload: any) {
    const typeData = {type: apidateType};
    return httpClient.post(DB_URL, {...payload.payload, ...typeData}).then(function(response) {
        customLog('created new doc ' + payload.sourceId + ': ' + response.data.id);
    }).catch(function(error) {
        customLog('failed to create doc ' + payload.sourceId + ': ' +
            (error.response ? error.response.status : error.message));
    });
}

function cloneDocument(apidateType: string, payload: any) {
    return httpClient.get(`${DB_URL}/_design/api/_list/details/by-external-id?key="${payload.sourceId}"&type=${apidateType}`)
        .then(function(response) {
            let clonedDoc = response.data;
            clonedDoc.externalId = payload.duplicatedObjectId;
            clonedDoc.userId = payload.userId;
            clonedDoc.updatedAt = new Date().getTime();
            return httpClient.post(DB_URL, clonedDoc).then(function(response) {
                customLog('created duplicate doc ' + payload.duplicatedObjectId + ': ' + response.data.id);
            }).catch(function(error) {
                customLog('failed to duplicate doc ' + payload.sourceId + ': ' +
                    (error.response ? error.response.status : error.message));
            });
        }).catch(function(error) {
            customLog('failed to retrieve doc ' + payload.sourceId + ': ' + (error.response ? error.response.status : error.message));
        });
}

function deleteDocument(apidateType: string, payload: any) {
    return httpClient.get(`${DB_URL}/_design/api/_list/ids/by-type-and-external-id?key=["${apidateType}","${payload.sourceId}"]`)
        .then(function(response) {
            const docsToDelete = response.data;
            let deletionRequests = [];
            for (const doc of docsToDelete) {
                deletionRequests.push(httpClient.delete(`${DB_URL}/${doc.id}?rev=${doc.rev}`));
            }
            axios.all(deletionRequests).then(function(response) {
                customLog('deleted doc ' + payload.sourceId);
            }).catch(function(error) {
                customLog('failed to delete doc ' + payload.sourceId + ': ' +
                    (error.response ? error.response.status : error.message));
            })
        }).catch(function(error) {
            customLog('failed to retrieve doc ' + payload.sourceId + ': ' + (error.response ? error.response.status : error.message));
        });
}

function updateDocument(apidateType: string, payload: any) {
    return httpClient.get(`${DB_URL}/_design/api/_view/by-type-and-external-id?key=["${apidateType}","${payload.sourceId}"]`)
        .then(function(response) {
            let resp_body = response.data;
            if (resp_body.rows && resp_body.rows.length === 1) {
                let docToUpdate = resp_body.rows[0].value;
                return httpClient.put(`${DB_URL}/${docToUpdate._id}?rev=${docToUpdate._rev}`,
                    {...docToUpdate, ...payload.payload}).then(function(response) {
                    customLog('updated doc ' + payload.sourceId + ': ' + response.data.id + ' | ' + response.data.rev);
                }).catch(function(error) {
                    customLog('failed to update doc ' + payload.sourceId + ': ' +
                        (error.response ? error.response.status : error.message));
                });
            } else {
                customLog('unexpected response: ' + resp_body);
                return Promise.reject('unexpected response: ' + resp_body);
            }
        }).catch(function(error) {
            customLog('failed to retrieve doc ' + payload.sourceId + ': ' + (error.response ? error.response.status : error.message));
        });
}
