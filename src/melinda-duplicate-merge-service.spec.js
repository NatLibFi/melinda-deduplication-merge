const chai = require('chai');
const expect = chai.expect;
const MarcRecord = require('marc-record-js');
const sinon = require('sinon');

const RecordUtils = require('melinda-deduplication-common/utils/record-utils');
const MelindaDuplicateMergeService = require('./melinda-duplicate-merge-service');
const MelindaMergeUpdate = require('melinda-deduplication-common/utils/melinda-merge-update');
const MergeValidation = require('melinda-deduplication-common/marc-record-merge-utils/marc-record-merge-validate-service');
const RecordMergeService = require('melinda-deduplication-common/utils/record-merge-service');

describe('MelindaDuplicateMergeService', () => {

  let melindaConnector, preferredRecordService, duplicateDatabaseConnector, recordMergeService;
  let record1;
  let record2;

  let commitMerge;
  let duplicateService;
  beforeEach(() => {
    
    record1 = new MarcRecord();
    record2 = new MarcRecord();
  
    melindaConnector = {
      loadRecord: sinon.stub(),
      loadSubrecords: sinon.stub(),
      createRecord: sinon.stub()
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

    commitMerge = sinon.stub();
    
    sinon.stub(MelindaMergeUpdate, 'commitMerge').callsFake(commitMerge);

    duplicateService = MelindaDuplicateMergeService.create(melindaConnector, preferredRecordService, duplicateDatabaseConnector, recordMergeService);
  });
  afterEach(() => {
    MelindaMergeUpdate.commitMerge.restore();
  });
  
  it('should call the commitMerge when everyting is ok', async () => {
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
    melindaConnector.loadSubrecords.resolves([]);

    commitMerge.resolves({
      recordId: 123
    });

    preferredRecordService.selectPreferredRecord.returns({ preferredRecord: record1, otherRecord: record2 });
    recordMergeService.mergeRecords.resolves({
      mergedRecordFamily: { 
        record: new MarcRecord()
      }
    });
    
    await duplicateService.handleDuplicate(fakeDuplicate);

    expect(commitMerge.callCount).to.equal(1);
  
  });
  
  it('should be manually mergeable if subrecords do not match', async () => {
    const fakeDuplicate = {
      first: { base: 'TST01', id: '00001'},
      second: { base: 'TST01', id: '00002'}
    };

    record1.appendField(RecordUtils.stringToField('001    00001'));
    record1.appendField(RecordUtils.stringToField('100    ‡aTekijä'));

    record2.appendField(RecordUtils.stringToField('001    00002'));
    record2.appendField(RecordUtils.stringToField('100    ‡aTekijä'));
    
    const componentRecord1 = MarcRecord.clone(record1);
    const componentRecord2 = MarcRecord.clone(record2);
    
    melindaConnector.loadRecord.withArgs('TST01', '00001').resolves(record1);
    melindaConnector.loadRecord.withArgs('TST01', '00002').resolves(record2);
    melindaConnector.loadSubrecords.withArgs('TST01', '00001').resolves([componentRecord1, componentRecord2]);
    melindaConnector.loadSubrecords.withArgs('TST01', '00002').resolves([componentRecord1]);

    commitMerge.resolves({
      recordId: 123
    });

    preferredRecordService.selectPreferredRecord.returns({ preferredRecord: record1, otherRecord: record2 });

    const error = new MergeValidation.MergeValidationError();
    error.mergeabilityClass = RecordMergeService.MergeabilityClass.MANUALLY_MERGEABLE;
    recordMergeService.mergeRecords.rejects(error);
    
    await duplicateService.handleDuplicate(fakeDuplicate);

    expect(commitMerge.callCount).to.equal(0);
    expect(duplicateDatabaseConnector.addDuplicatePair.callCount).to.equal(1);

  });
});
