// @flow

const logger = require('melinda-deduplication-common/utils/logger');
logger.log('info', 'Starting melinda-deduplication-merge');

const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');

const utils = require('melinda-deduplication-common/utils/utils');
const DuplidateQueueConnector = require('melinda-deduplication-common/utils/duplicate-queue-connector');
const DuplicateDatabaseConnector = require('melinda-deduplication-common/utils/duplicate-database-connector');
const MelindaConnector = require('melinda-deduplication-common/utils/melinda-record-service');

const RecordMergeService = require('melinda-deduplication-common/utils/record-merge-service');
const PreferredRecordService = require('melinda-deduplication-common/utils/preferred-record-service');

const MelindaDuplicateMergeService = require('./melinda-duplicate-merge-service');

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

const NOOP = utils.readEnvironmentVariable('NOOP', false) != false;

const DUPLICATE_QUEUE_AMQP_URL = utils.readEnvironmentVariable('DUPLICATE_QUEUE_AMQP_URL');

const mergeConfiguration = require('./config/merge-config');
const componentRecordMatcherConfiguration = require('./config/component-record-similarity-definition.js');
const modelPath = path.resolve(__dirname, 'config', 'select-better-model.json');
const selectPreferredRecordModel = JSON.parse(fs.readFileSync(modelPath, 'utf8'));

const createTimer = require('melinda-deduplication-common/utils/start-stop-timer');

const ONLINE = utils.readEnvironmentVariable('ONLINE', '00:00-21:45, 22:30-24:00');

const service = createService();

const onlinePoller = createTimer(ONLINE, service, logger);

process.on('SIGTERM', async () => {
  logger.log('info', 'SIGTERM received');
  service.stop();
  clearInterval(onlinePoller);
  logger.log('info', 'Exiting');
});

function createService() {

  let duplicateQueueConnection;
  let duplicateChannel;

  return {
    start: async() => {

      logger.log('info', `Connecting to ${DUPLICATE_QUEUE_AMQP_URL}`);
      duplicateQueueConnection = await amqp.connect(DUPLICATE_QUEUE_AMQP_URL);
      duplicateChannel = await duplicateQueueConnection.createChannel();
      const duplicateQueueConnector = DuplidateQueueConnector.createDuplicateQueueConnector(duplicateChannel);
      logger.log('info', 'Connected');

      
      const duplicateDatabaseConnector = DuplicateDatabaseConnector.createDuplicateDatabaseConnector(duplicateDBConfiguration);
      const melindaConnector = MelindaConnector.createMelindaRecordService(MELINDA_API, X_SERVER, MELINDA_CREDENTIALS);

      const recordMergeService = RecordMergeService.createRecordMergeService(mergeConfiguration, componentRecordMatcherConfiguration);
      const preferredRecordService = PreferredRecordService.createPreferredRecordService(selectPreferredRecordModel);
    
      const melindaDuplicateMergeService = MelindaDuplicateMergeService.create(melindaConnector, preferredRecordService, duplicateDatabaseConnector, recordMergeService, { logger: logger, noop: NOOP });

      duplicateQueueConnector.listenForDuplicates(async (duplicate, done) => {

        try { 
          await melindaDuplicateMergeService.handleDuplicate(duplicate);
          return done();
        } catch(error) {
          logger.log('error', error.message, error);
          logger.log('info', 'Waiting 60 seconds before continuing');
          setTimeout(() => {
            return done();
          }, 60000);
          
        }
      });
    },

    stop: async() => {
      duplicateChannel && await duplicateChannel.close();
      duplicateQueueConnection && await duplicateQueueConnection.close();
      logger.log('info', 'Connections released.');
    }
  };
}
