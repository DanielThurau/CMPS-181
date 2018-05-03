#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int test_delete_record(RecordBasedFileManager *rbfm) {
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
    prepareRecord(recordDescriptor.size(), nullFieldIndicator, 6, "Morgan", 20, 6.25, 20000, record_2, &record_2_size);

    rbfm->printRecord(recordDescriptor, record_1);

    rbfm->insertRecord(fileHandle, recordDescriptor, record_1, rid_1);
    rbfm->insertRecord(fileHandle, recordDescriptor, record_1, rid_1);
    rbfm->insertRecord(fileHandle, recordDescriptor, record_2, rid_2);
    rbfm->insertRecord(fileHandle, recordDescriptor, record_2, rid_2);

    RBFM_ScanIterator scanner;
    void *conditionName = malloc(20);
    *((unsigned*) conditionName) = 7;
    memcpy((uint8_t*) conditionName + 4, "Morgann", 7);
    string arr[] = {"Age", "Height", "Salary"};
    vector<string> attributes(arr, arr + sizeof(arr) / sizeof(arr[0]));
    rbfm->scan(fileHandle, recordDescriptor, "EmpName", LT_OP, conditionName, attributes, scanner);

    RID returnedRID;

    recordDescriptor.erase(recordDescriptor.begin());
    while (scanner.getNextRecord(returnedRID, returned_data) != RBFM_EOF) {
        cout << "page: " << returnedRID.pageNum << " slot: " << returnedRID.slotNum << endl;
        rbfm->printRecord(recordDescriptor, returned_data);
    }

    free(record_1);
    free(record_2);

    return 0;
}

int main()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test_ur");
       
    RC rcmain = test_delete_record(rbfm);
    return rcmain;
}
