import request = require('request');
import kafka = require('kafka-node');
import {DB_PASSWORD, DB_URL, DB_USER, KAFKA_HOST} from "./config";

const basicClient = new kafka.KafkaClient({kafkaHost: KAFKA_HOST});

if (process.argv.length === 3) {
    let typeArg = process.argv[2];
    const apidateType = typeArg.split('=')[1];
    console.log('Trying to subscribe to topic ' + apidateType);
    startProcessing(apidateType);
} else {
    console.log('unsupported args: ' + process.argv.join(' '));
    process.exit(1);
}

function startProcessing(apidateType: string) {
    const fetchRequests = [{topic: apidateType}];
    const consumer = new kafka.Consumer(basicClient, fetchRequests, {autoCommit: true});
    console.log('Successfully subscribed to topic ' + apidateType);
    consumer.on('error', (error: Error) => {
        console.log('consumer error : ' + error.message);
    });
    consumer.on('offsetOutOfRange', (error: Error) => {
        console.log('consumer offsetOutOfRange : ' + error.message);
    });
    consumer.on('message', (message: kafka.Message) => {
        try {
            let payload = JSON.parse(message.value as string);
            if (payload.operation === 'DUPLICATE_PERIOD') {
                cloneDocument(apidateType, payload);
            } else if (payload.operation === 'DELETE_PERIOD') {
                deleteDocument(apidateType, payload);
            } else {
                console.log('unsupported operation: ' + payload.operation);
            }
        } catch (e) {
            console.log('invalid message value : ' + message.value);
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
                        console.log('created duplicate doc ' + payload.duplicatedObjectId + ': ' + bdy.id);
                    } else {
                        console.log('failed to duplicate doc ' + payload.sourceObjectId + ': ' +
                            resp.statusCode + ' - ' + (err ? err.message : 'unknown error'));
                    }
                });
            } else {
                console.log('failed to retrieve doc ' + payload.sourceObjectId + ': ' +
                    response.statusCode + ' - ' + (error ? error.message : 'unknown error'));
            }
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
                                console.log('deleted doc ' + payload.periodId + ': ' + respBody.id + ' | ' + respBody.rev);
                            } else {
                                console.log('failed to delete doc ' + payload.periodId + ': ' +
                                    resp.statusCode + ' - ' + (err ? err.message : 'unknown error'));
                            }
                        });
                }
            } else {
                console.log('failed to retrieve doc ' + payload.periodId + ': ' +
                    response.statusCode + ' - ' + (error ? error.message : 'unknown error'));
            }
        });
}
