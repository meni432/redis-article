const Redis = require('ioredis');
const { FirehoseClient, PutRecordCommand } = require("@aws-sdk/client-firehose");

const firehoseClient = new FirehoseClient({ region: process.env.AWS_REGION });

// Initialize Redis client
const redis = new Redis({ 
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
});

const THRESHOLD_DISTANCE = 100; // Distance threshold in meters

exports.handler = async (event) => {
    const records = event.Records;

    try {
        for (const record of records) {
            // Decode base64 encoded record data
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8');
            const body = JSON.parse(payload);

            const userID = body.userID;
            const lat = body.lat;
            const lng = body.lng;

            if (userID === undefined || lat === undefined || lng === undefined)
            {
                console.error('Invalid payload', payload);
                continue;
            }

            const lastLocationJSON = await redis.get(userID);
            
            if (lastLocationJSON)
            {
                const lastLocation = JSON.parse(lastLocationJSON);
                if (distance(lastLocation.lat, lastLocation.lng, lat, lng) > THRESHOLD_DISTANCE)
                {
                    // Send notification to user
                    console.log(`User ${userID} has moved more than ${THRESHOLD_DISTANCE} meters`);

                    // save to kinesis firehose
                    await saveToKinesis(userID, lat, lng);
                }
                else
                {
                    console.log(`User ${userID} has not moved`);
                }
            } else {
                console.log(`User ${userID} is new`);
                await saveToKinesis(userID, lat, lng);
            }
            
            // Process and insert into Redis (example: using payload as key-value)
            await redis.set(userID, payload);
        }
    } catch (error) {
        console.error('Error processing records', error);
        throw error;
    }

    async function saveToKinesis(userID, lat, lng) {
        const data = {
            userID: userID,
            lat: lat,
            lng: lng
        };

        const dataAsBase64 = Buffer.from(JSON.stringify(data));

        const input = {
            DeliveryStreamName: process.env.KINESIS_FIREHOSE_NAME,
            Record: {
                Data: dataAsBase64
            }
        };

        try {
            const data = await firehoseClient.send(new PutRecordCommand(input));
            console.log('Successfully sent message:', data);
        } catch (err) {
            console.error('Error sending message to Kinesis Firehose:', err);
        }
    }
};

function distance(lat1, lng1, lat2, lng2) {
    // https://stackoverflow.com/a/27943/1293256
    const p = 0.017453292519943295;    // Math.PI / 180
    const c = Math.cos;
    const a = 0.5 - c((lat2 - lat1) * p)/2 + 
              c(lat1 * p) * c(lat2 * p) * 
              (1 - c((lng2 - lng1) * p))/2;
  
    return 12742 * Math.asin(Math.sqrt(a)); // 2 * R; R = 6371 km
}