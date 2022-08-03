const BlobServiceClient = require('@azure/storage-blob').BlobServiceClient;
const openpgp = require('openpgp');
const fs = require('fs');
const Transform = require('stream').Transform;
const Writable = require('stream').Writable;
const Kafka = require('kafkajs').Kafka;

module.exports = async function (context, myBlob) {
    const AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=storage25;AccountKey=B1gcL0RZjdxsQ6BhxFUYKDidTNBVrw2y01qlqtVqDBgYFbyBEA9HKqZvh9vVDbK6zwhsb1siU6yt+AStk5NDYA==;EndpointSuffix=core.windows.net';
    const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);

    const containerName = 'files';
    const containerClient = blobServiceClient.getContainerClient(containerName);

    // const blobName = 'gap_uat_profile_extract_huge_2021053111050202.csv.pgp';
    const blobName = 'gap_uat_profile_extract_2022042001000203.csv.gpg';
    const blobClient = containerClient.getBlockBlobClient(blobName);
    const keyBlobName = 'HQ-SELL-LOYALTY-DG_Private_key.asc';
    const keyBlobClient = containerClient.getBlockBlobClient(keyBlobName);

    const encryptedDataStream = (await blobClient.download(0)).readableStreamBody;
    encryptedDataStream.setEncoding('utf-8');


    const passphrase = 'Loy@ltymtlaccountmatch';
    const keyStream = (await keyBlobClient.download(0)).readableStreamBody;
    keyStream.setEncoding('utf-8');
    // const privateKeyArmored = fs.readFileSync('PrivateKey.asc', 'utf-8');

    let privateKeyArmored = '';

    for await (const chunk of keyStream) {
        privateKeyArmored += Buffer.from(chunk).toString();
    }

    const privateKey = await openpgp.decryptKey({
        privateKey: await openpgp.readPrivateKey({armoredKey: privateKeyArmored}),
        passphrase
    });
    
    const decryptedData = await openpgp.decrypt({
        message: await openpgp.readMessage({armoredMessage: encryptedDataStream}),
        decryptionKeys: privateKey,
        config: {allowUnauthenticatedStream: true}
    });

       const kafka = new Kafka({
   
        ssl: true,
        brokers: ['pkc-7prvp.centralindia.azure.confluent.cloud:9092'],
        clientId: 'my-app',
        sasl: {
            mechanism: 'plain',
            username: 'JETUVEKU5LCQUDLO',                                                           
            password: 'UU3gYgkmo9vlWcV3jNT6ZTZm2p0/Hzl4f97SR6AqC149XGZfnRX7j/iUHgaLV0oB'             
          },
          connectionTimeout: 5000
           
      });
    const producer = kafka.producer();
    await producer.connect();


const t = new Transform({objectMode: true, 
    transform(chunk, encoding, done) {
        let data = chunk.toString();
        if (this._lastLineData)
        data = this._lastLineData + data;

        let lines = data.split('\n');
        this._lastLineData = lines.splice(lines.length-1, 1)[0];

        lines.forEach(this.push.bind(this));
        done();
    },
    flush(done){
        if (this._lastLineData) 
        this.push(this._lastLineData)
        this._lastLineData = null;
        done();
   }});

    // const indices = [0, 26, 29];
    // const headers = ['loyaltyMemberId', 'eventDateTime', 'currentTier'];
    let lineNo = 0;

    let obj = {
        'loyaltyMemberId': '',
        'eventDateTime': '',
        'publishDateTime': '',
        'currentTier': '',
        'marketCode': ''
    };

    const w = new Writable({
        write: async(chunk, encoding, done) => {
            if (lineNo < 2) {
                lineNo++;
            } else {
                const fields = (chunk.toString()).split(',');

                obj['loyaltyMemberId'] = fields[0];
                obj['eventDateTime'] = fields[26];
                obj['publishDateTime'] = (new Date()).toLocaleString();
                obj['currentTier'] = fields[29];
                obj['marketCode'] = 'US';

                // console.log(obj);

                await producer.send({
                    topic: 'data',
                    messages: [{value: JSON.stringify(obj)}]
                });
            }
            done();
        }
    });

    w.on('finish', () => {process.exit(1)});
    
   decryptedData.data.pipe(t).pipe(w);
};