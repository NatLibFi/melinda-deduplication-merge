// @flow

import type { Duplicate } from 'melinda-deduplication-common/types/duplicate.flow';

export type MelindaDuplicateMergeService = {
  handleDuplicate: (duplicate: Duplicate) => Promise<any>
};
