const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

/**
 * A Lambda function that will be triggered every time new object is put into the s3 bucket, So we do not need to use cronjob
 * Test: sam local invoke S3PokeFunction -e .\events\event-s3.json
 */
exports.s3PokeHandler = async (event, context) => {
  // Step 1: Read content of CSV file
  const s3Record = event.Records[0];
  const bucketName = s3Record.s3.bucket.name;
  const csvKey = s3Record.s3.object.key;
  const params = {
    Bucket: bucketName,
    Key: csvKey
  };
  const csvObjectRes = await s3.getObject(params).promise();
  const csvStr = csvObjectRes.Body.toString();
  const csvData = parseCsv(csvStr);
  // Step 2: Filtering Pokemons
  // Use Promise.all combine with axios/fetch to call multiple requests at once, in order to save time

  // Step 3: Batch Write qualified poke into DB
  // const batchWriteparams = {
  //   RequestItems: {
  //     "TABLE_NAME": csvData.map(csvObj => {
  //       'PutRequest': {
  //         'Item': {
  //           id: csvObj.id,
  //           is_default: csvData.is_default,
  //           base_experience: csvObj.base_experience,
  //           name: csvObject.name,
  //         }
  //       }
  //     })
  //   }
  // };
  
  // await dynamodb.batchWriteItem(params);

  // Step 4: Put result text into S3
  const dateFromSource = csvKey.split('.')[0].split('_')[1];
  const content_text = `Import executed. Number of Pokemons processed is ${csvData.length}`;
  const putResultParams = {
    Bucket: bucketName,
    Key: `result_${dateFromSource}.txt`,
    Body: Buffer.from(content_text, 'binary')
  }
  await s3.putObject(putResultParams).promise();
};


const parseCsv = (csvstr) => {
  const lines = csvstr.split('\n');
  const headers = lines[0].split(',');
  const parsedCsv = [];
  for(let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',');
    const csvObj = {}
    headers.forEach((header, idx) => csvObj[header] = values[idx]);
    parsedCsv.push(csvObj)
  }

  return parsedCsv;
}