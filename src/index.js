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
const DUPLICATE_DB_MESSAGE = utils.readEnvironmentVariable('DUPLICATE_DB_MESSAGE', 'Automatic Melinda deduplication');
const DUPLICATE_DB_PRIORITY = utils.readEnvironmentVariable('DUPLICATE_DB_PRIORITY', 1);

const duplicateDBConfiguration = {
  endpoint: DUPLICATE_DB_API,
  messageForDuplicateDatabase: DUPLICATE_DB_MESSAGE,
  priorityForDuplicateDatabase: DUPLICATE_DB_PRIORITY
};

const MELINDA_API = utils.readEnvironmentVariable('MELINDA_API', 'http://libtest1.csc.fi:8992/API');
const X_SERVER = utils.readEnvironmentVariable('X_SERVER', 'http://libtest1.csc.fi:8992/X');
const MELINDA_USERNAME = utils.readEnvironmentVariable('MELINDA_USERNAME', '');
const MELINDA_PASSWORD = utils.readEnvironmentVariable('MELINDA_PASSWORD', '');
const MELINDA_CREDENTIALS = {
  username: MELINDA_USERNAME,
  password: MELINDA_PASSWORD
};

const DUPLICATE_QUEUE_AMQP_URL = utils.readEnvironmentVariable('DUPLICATE_QUEUE_AMQP_URL');

const mergeConfiguration = require('./config/merge-config');

start().catch(error => {
  logger.log('error', error.message, error);
});

async function start() {
  logger.log('info', `Connecting to ${DUPLICATE_QUEUE_AMQP_URL}`);
  const duplicateQueueConnection = await amqp.connect(DUPLICATE_QUEUE_AMQP_URL);
  const duplicateChannel = await duplicateQueueConnection.createChannel();
  const duplicateQueueConnector = DuplidateQueueConnector.createDuplicateQueueConnector(duplicateChannel);
  logger.log('info', 'Connected');

  const duplicateDatabaseConnector = DuplicateDatabaseConnector.createDuplicateDatabaseConnector(duplicateDBConfiguration);
  const melindaConnector = MelindaConnector.createMelindaRecordService(MELINDA_API, X_SERVER, MELINDA_CREDENTIALS);

  const recordMergeService = RecordMergeService.createRecordMergeService(mergeConfiguration, melindaConnector, logger);

  duplicateQueueConnector.listenForDuplicates(async (duplicate, done) => {
    const pairIdentifierString = `${duplicate.first.base}/${duplicate.first.id} - ${duplicate.second.base}/${duplicate.second.id}`;
    logger.log('info', `Handling duplicate pair ${pairIdentifierString}`);

    logger.log('info', `Loading records from ${MELINDA_API}`);
    try {
      const firstRecord = await melindaConnector.loadRecord(duplicate.first.base, duplicate.first.id);
      const secondRecord = await melindaConnector.loadRecord(duplicate.second.base, duplicate.second.id);

      //TODO these might be deleted records. No action is required for deleted records.
      
      logger.log('info', `Records are: ${duplicate.first.base}/${selectRecordId(firstRecord)} and ${duplicate.second.base}/${selectRecordId(secondRecord)}`);
      

      // check if duplicate can be merged automatically
      //TODO this may also fail in situations where manual merge is impossible. Instead of 2, this needs to be split into 3 classes: automergeable, manualmergeable, unmergeable. unmergeables need no action.
      const automaticMergePossible = await recordMergeService.checkMergeability(firstRecord, secondRecord);

      if (!automaticMergePossible) {
        logger.log('warn', `Duplicate pair ${pairIdentifierString} cannot be merged automatically. Pair will be sent to: ${DUPLICATE_DB_API}`);
        try {
          await duplicateDatabaseConnector.addDuplicatePair(duplicate.first, duplicate.second);
        } catch(error) {
          logger.log('warn', `Could not add ${pairIdentifierString} to duplicate database: ${error.message}`);
        }
        return done();
      }

      const mergeResult = await recordMergeService.mergeRecords(firstRecord, secondRecord);
      
      const mergedRecordIdentifier = `${mergeResult.record.base}/${mergeResult.record.id}`;
      logger.log('info', `Duplicate pair ${pairIdentifierString} has been merged to ${mergedRecordIdentifier}`);

      done();

    } catch(error) {
      // error may be
      // loadRecord error
      if (error.name === 'AlephRecordError') {
        logger.log('error', error.message);
        return;
      }
      
      // merging error?
      // type error/programming error

      logger.log('error', error.message, error);
      
    }
  });
}

function selectRecordId(record) {
  return _.get(record.fields.find(field => field.tag === '001'), 'value');
}
