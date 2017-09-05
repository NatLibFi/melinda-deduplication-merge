const chai = require('chai');
const expect = chai.expect;
const MarcRecord = require('marc-record-js');
const sinon = require('sinon');

const RecordUtils = require('melinda-deduplication-common/utils/record-utils');
const MelindaDuplicateMergeService = require('./melinda-duplicate-merge-service');

describe('MelindaDuplicateMergeService', () => {

  let melindaConnector, preferredRecordService, duplicateDatabaseConnector, recordMergeService;
  let record1;
  let record2;

  let duplicateService;
  beforeEach(() => {
    
    record1 = new MarcRecord();
    record2 = new MarcRecord();
  
    melindaConnector = {
      loadRecord: sinon.stub()
    };

    preferredRecordService = {
      selectPreferredRecord: sinon.stub()
    };

    duplicateDatabaseConnector = {
      addDuplicatePair: sinon.stub()
    };

    recordMergeService = {
      mergeRecords: sinon.stub()
    };

    duplicateService = MelindaDuplicateMergeService.create(melindaConnector, preferredRecordService, duplicateDatabaseConnector, recordMergeService);
  });
  
  it('should call the mergeRecords service when everyting is ok', async () => {
    const fakeDuplicate = {
      first: { base: 'TST01', id: '00001'},
      second: { base: 'TST01', id: '00002'}
    };

    record1.appendField(RecordUtils.stringToField('001    00001'));
    record1.appendField(RecordUtils.stringToField('100    ‡aTekijä'));

    record2.appendField(RecordUtils.stringToField('001    00002'));
    record2.appendField(RecordUtils.stringToField('100    ‡aTekijä'));
    
    melindaConnector.loadRecord.withArgs('TST01', '00001').resolves(record1);
    melindaConnector.loadRecord.withArgs('TST01', '00002').resolves(record2);

    preferredRecordService.selectPreferredRecord.returns({ preferredRecord: record1, otherRecord: record2 });
    recordMergeService.mergeRecords.resolves({ record: { base: 'TST01', id: '00003'}});
    
    await duplicateService.handleDuplicate(fakeDuplicate);

    expect(recordMergeService.mergeRecords.callCount).to.equal(1);
  
  });

});