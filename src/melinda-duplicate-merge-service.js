// @flow
import type { MelindaDuplicateMergeService } from './melinda-duplicate-merge-service.flow';
import type { MelindaRecordService } from 'melinda-deduplication-common/types/melinda-record-service.flow';
import type { PreferredRecordService } from 'melinda-deduplication-common/types/preferred-record-service.flow';
import type { DuplicateDatabaseConnector } from 'melinda-deduplication-common/types/duplicate-database-connector.flow';
import type { RecordMergeService } from 'melinda-deduplication-common/types/record-merge-service.flow';


const debug = require('debug')('melinda-duplicate-merge-service');
const _ = require('lodash');

const RecordMergeCheck = require('melinda-deduplication-common/utils/record-merge-check');

const DEFAULT_LOGGER = { log: debug };

function create(melindaConnector: MelindaRecordService, 
  preferredRecordService: PreferredRecordService, 
  duplicateDatabaseConnector: DuplicateDatabaseConnector, 
  recordMergeService: RecordMergeService, 
  options: mixed): MelindaDuplicateMergeService {

  const logger = _.get(options, 'logger', DEFAULT_LOGGER);
  
  async function handleDuplicate(duplicate) {
    const pairIdentifierString = `${duplicate.first.base}/${duplicate.first.id} - ${duplicate.second.base}/${duplicate.second.id}`;
    logger.log('info', `Handling duplicate pair ${pairIdentifierString}`);

    logger.log('info', 'Loading records from Aleph');
    try {
      const loadRecordOptions = { handle_deleted: 1, no_rerouting: 1 };
      const firstRecord = await melindaConnector.loadRecord(duplicate.first.base, duplicate.first.id, loadRecordOptions);
      const secondRecord = await melindaConnector.loadRecord(duplicate.second.base, duplicate.second.id, loadRecordOptions);

      if (isDeleted(firstRecord)) {
        logger.log('info', `Record ${duplicate.first.base}/${duplicate.first.id} is deleted.`);
        return;
      }
      if (isDeleted(secondRecord)) {
        logger.log('info', `Record ${duplicate.second.base}/${duplicate.second.id} is deleted.`);
        return;
      }

      const firstRecordId = selectRecordId(firstRecord);
      const secondRecordId = selectRecordId(secondRecord);

      logger.log('info', `Records are: ${duplicate.first.base}/${firstRecordId} and ${duplicate.second.base}/${secondRecordId}`);
      
      if (firstRecordId === secondRecordId) {
        logger.log('info', 'Pair resolves to same record. Nothing to do.');
        return;
      }

      const { preferredRecord, otherRecord } = preferredRecordService.selectPreferredRecord(firstRecord, secondRecord);
      logger.log('info', `Selected ${selectRecordId(preferredRecord)} as preferred record.`);
      
      const mergeability = await RecordMergeCheck.checkMergeability(preferredRecord, otherRecord);

      logger.log('info', `Mergeability class: ${mergeability}.`);
      if (mergeability === RecordMergeCheck.MergeabilityClass.NOT_MERGEABLE) {
        logger.log('warn', `Duplicate pair ${pairIdentifierString} is not mergeable.`);
        return;
      }
      if (mergeability === RecordMergeCheck.MergeabilityClass.MANUALLY_MERGEABLE) {
        logger.log('warn', `Duplicate pair ${pairIdentifierString} cannot be merged automatically. Pair will be sent to duplicate database`);
        try {
          await duplicateDatabaseConnector.addDuplicatePair(duplicate.first, duplicate.second);
        } catch(error) {
          logger.log('warn', `Could not add ${pairIdentifierString} to duplicate database: ${error.message}`);
        }
        return;
      }
      logger.log('info', `Duplicate pair ${pairIdentifierString} is mergeable automatically. Merging.`);
      
      const mergeResult = await recordMergeService.mergeRecords({ preferredRecord, otherRecord });
      
      const mergedRecordIdentifier = `${mergeResult.record.base}/${mergeResult.record.id}`;
      logger.log('info', `Duplicate pair ${pairIdentifierString} has been merged to ${mergedRecordIdentifier}`);

    } catch(error) {
      // error may be
      // loadRecord error
      if (error.name === 'AlephRecordError') {
        logger.log('error', error.message);
        return;
      }
      
      // merging error?
      // type error/programming error
      throw error;
    }
  }
  return {
    handleDuplicate
  };
}

function selectRecordId(record) {
  return _.get(record.fields.find(field => field.tag === '001'), 'value');
}

function isDeleted(record) {
  return record.leader.substr(5,1) === 'd';
}

module.exports = {
  create
};

