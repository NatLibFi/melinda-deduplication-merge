// @flow

const logger = require('melinda-deduplication-common/utils/logger');
logger.log('info', 'Starting melinda-deduplication-merge');

const _ = require('lodash');
const amqp = require('amqplib');

const utils = require('melinda-deduplication-common/utils/utils');
const DuplidateQueueConnector = require('melinda-deduplication-common/utils/duplicate-queue-connector');
const DuplicateDatabaseConnector = require('melinda-deduplication-common/utils/duplicate-database-connector');
const MelindaConnector = require('melinda-deduplication-common/utils/melinda-record-service');

const RecordMergeService = require('melinda-deduplication-common/utils/record-merge-service');

const DUPLICATE_DB_API = utils.readEnvironmentVariable('DUPLICATE_DB_API');
const MELINDA_API = utils.readEnvironmentVariable('MELINDA_API', 'http://libtest1.csc.fi:8992/API');
const X_SERVER = utils.readEnvironmentVariable('X_SERVER', 'http://libtest1.csc.fi:8992/X');
const MELINDA_USERNAME = utils.readEnvironmentVariable('MELINDA_USERNAME', '');
const MELINDA_PASSWORD = utils.readEnvironmentVariable('MELINDA_PASSWORD', '');
const MELINDA_CREDENTIALS = {
  username: MELINDA_USERNAME,
  password: MELINDA_PASSWORD
};

const DUPLICATE_QUEUE_AMQP_HOST = utils.readEnvironmentVariable('DUPLICATE_QUEUE_AMQP_HOST');

start().catch(error => {
  logger.log('error', error.message, error);
});

async function start() {
  logger.log('info', `Connecting to ${DUPLICATE_QUEUE_AMQP_HOST}`);
  const duplicateQueueConnection = await amqp.connect(DUPLICATE_QUEUE_AMQP_HOST);
  const duplicateChannel = await duplicateQueueConnection.createChannel();
  const duplicateQueueConnector = DuplidateQueueConnector.createDuplicateQueueConnector(duplicateChannel);
  logger.log('info', 'Connected');

  const duplicateDatabaseConnector = DuplicateDatabaseConnector.createDuplicateDatabaseConnector(DUPLICATE_DB_API);
  const melindaConnector = MelindaConnector.createMelindaRecordService(MELINDA_API, X_SERVER, MELINDA_CREDENTIALS);

  const recordMergeService = RecordMergeService.createRecordMergeService(melindaConnector);

  duplicateQueueConnector.listenForDuplicates(async (duplicate, done) => {
    const pairIdentifierString = `${duplicate.first.base}/${duplicate.first.id} - ${duplicate.second.base}/${duplicate.second.id}`;
    logger.log('info', `Handling duplicate pair ${pairIdentifierString}`);

    // Load records from melinda
    logger.log('info', `Loading records from ${MELINDA_API}`);
    const firstRecord = await melindaConnector.loadRecord(duplicate.first.base, duplicate.first.id);
    const secondRecord = await melindaConnector.loadRecord(duplicate.second.base, duplicate.second.id);

    // check if duplicate can be merged automatically
    const automaticMergePossible = await recordMergeService.checkMergeability(firstRecord, secondRecord);

    if (!automaticMergePossible) {
      // CANNOT_MERGE send duplicate to duplicate db
      await duplicateDatabaseConnector.addDuplicatePair(duplicate.first, duplicate.second);
      logger.log('warn', `Duplicate pair ${pairIdentifierString} cannot be merged automatically. Pair has been sent to: ${DUPLICATE_DB_API}`);
      return done();
    }

    // CAN_MERGE create merged record
    const mergeResult = await recordMergeService.mergeRecords(firstRecord, secondRecord);
    
    // save merged record to melinda
    const mergedRecordIdentifier = `${mergeResult.record.base}/${mergeResult.record.id}`;

    // mark old records as deleted
    logger.log('info', `Duplicate pair ${pairIdentifierString} has been merged to ${mergedRecordIdentifier}`);
    done();

  });

}
