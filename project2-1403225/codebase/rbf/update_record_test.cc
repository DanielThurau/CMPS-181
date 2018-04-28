#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int test_delete_record(RecordBasedFileManager *rbfm) {
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    string fileName = "test_ur";

    RC rc;
    rc = rbfm->createFile(fileName);
	rc = createFileShouldSucceed(fileName);
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);

    RID rid_1;
    RID rid_2;
    void *record_1 = malloc(100);
    void *record_2 = malloc(100);
    int record_1_size;
    int record_2_size;

    void *returned_data = malloc(100);
    
    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    int numNullBytes = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char nullFieldIndicator[numNullBytes];
    memset(nullFieldIndicator, 0, numNullBytes);

    prepareRecord(recordDescriptor.size(), nullFieldIndicator, 7, "Preston", 21, 5.917, 150000, record_1, &record_1_size);
    prepareRecord(recordDescriptor.size(), nullFieldIndicator, 6, "Morgan", 21, 6.25, 20000, record_2, &record_2_size);

    rbfm->insertRecord(fileHandle, recordDescriptor, record_1, rid_1);
    rbfm->insertRecord(fileHandle, recordDescriptor, record_1, rid_2);

    rbfm->updateRecord(fileHandle, recordDescriptor, record_2, rid_1);

    rbfm->readRecord(fileHandle, recordDescriptor, rid_1, returned_data);
    if (memcmp(record_2, returned_data, record_2_size) != 0) {
        cout << "record 2 didn't return correctly" << endl;
        return 1;
    }

    rbfm->readRecord(fileHandle, recordDescriptor, rid_2, returned_data);
    if (memcmp(record_1, returned_data, record_1_size) != 0) {
        cout << "record 1 didn't return correctly" << endl;
        return 1;
    }

    free(record_1);
    free(record_2);

    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test_ur");
       
    RC rcmain = test_delete_record(rbfm);
    return rcmain;
}
