const AWS = require('aws-sdk');

// Configure AWS region (e.g., 'us-west-2')
AWS.config.update({region: 'eu-west-1'});

// Create a Kinesis service object
const kinesis = new AWS.Kinesis({apiVersion: '2013-12-02'});

const streamName = 'KinesisIngressStream';

// Function to send message to Kinesis
const sendMessageToKinesis = async (userLocationData) => {
  const params = {
    Data: JSON.stringify(userLocationData), // Assuming message is an object
    PartitionKey: userLocationData.userID,
    StreamName: streamName,
  };

  try {
    const data = await kinesis.putRecord(params).promise();
    console.log('Successfully sent message:', data);
  } catch (err) {
    console.error('Error sending message to Kinesis:', err);
  }
};


sendMessageToKinesis({ lat: 13.3, lng: 14.33, userID: 'def' });

// Generate random user ID
const generateUserID = () => {
  const characters = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let userID = '';
  for (let i = 0; i < 5; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    userID += characters[randomIndex];
  }
  return userID;
};

// Generate random start location
const generateStartLocation = () => {
  const latitude = Math.random() * 90;
  const longitude = Math.random() * 180;
  return { lat: latitude.toFixed(2), lng: longitude.toFixed(2) };
};

// Generate random location with small changes
const generateRandomLocation = (startLocation) => {
  const latitudeChange = (Math.random() - 0.5) * 0.0001; // Random change within +/- 0.0001
  const longitudeChange = (Math.random() - 0.5) * 0.0001; // Random change within +/- 0.0001
  const newLatitude = parseFloat(startLocation.lat) + latitudeChange;
  const newLongitude = parseFloat(startLocation.lng) + longitudeChange;
  return { lat: newLatitude.toFixed(6), lng: newLongitude.toFixed(6) };
};

// Function to send multiple locations to Kinesis
const sendMultipleLocationsToKinesis = async (startLocation, numLocations) => {
  const userID = generateUserID();
  console.log(`Sending ${numLocations} locations for user ${userID}`)
  const sendingResult = Array.of(numLocations).map(() => {
    const location = generateRandomLocation(startLocation);
    const message = { lat: location.lat, lng: location.lng, userID: userID };
    return sendMessageToKinesis(message);
  });

  return Promise.all(sendingResult);
};

async function main() {
  while (true)
  {
    for (let i = 0; i < 1000; i++) {
      const startLocation = generateStartLocation();
      const numberOfLocations = Math.floor(Math.random() * 100) + 100; // Random number between 100 and 200
      sendMultipleLocationsToKinesis(startLocation, numberOfLocations);
    }
    // Wait for 1 second
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

main()

