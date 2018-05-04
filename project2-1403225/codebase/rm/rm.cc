#include "rm.h"
#include <cmath>

const int success = 0;
RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::_rbf_manager = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{

  tablesFileName = "Tables.tbl";
  columnsFileName = "Columns.tbl";
  tableCounter = 1;

  createCatalog();



}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
  cout << "Im being called" << endl;
    /*
      check if these tables exist in the file
    */ 
    RC rc;
    rc = _rbf_manager->createFile(tablesFileName);
    if(rc != success){
      return rc;
    }

    FileHandle fileHandle;
    rc = _rbf_manager->openFile(tablesFileName, fileHandle);
    if(rc != success){
      return rc;
    }

    vector<Attribute> tableDescriptor;
    createTableDescriptor(tableDescriptor);


    void *tuple = malloc(200);
    void * returned = malloc(200);


    int tableID = tableCounter++;
    prepareTables(tableID, "Tables", "Tables.tbl", tuple, tableDescriptor);
    RID tablesRID;
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, tuple, tablesRID);


    _rbf_manager->readRecord(fileHandle, tableDescriptor, tablesRID, returned);
    _rbf_manager->printRecord(tableDescriptor, returned);


    int columnID = tableCounter++;
    prepareTables(columnID, "Columns", "Columns.tbl", tuple, tableDescriptor);
    RID columnsRID;
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, tuple, columnsRID);
    

    rc = _rbf_manager->closeFile(fileHandle);
    if(rc != success){
      return rc;
    }


    // rc = createCatalogColumns(tableID, columnID);
    // if(rc != success){
    //   return rc;
    // }




    return success;
}

RC RelationManager::deleteCatalog()
{
    RC rc;
    rc = _rbf_manager->destroyFile(tablesFileName);
    if(rc)
      return rc;

    rc = _rbf_manager->destroyFile(columnsFileName);
    if(rc)
      return rc;

    return success;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    FileHandle fileHandle;
    rc = _rbf_manager->openFile(tablesFileName, fileHandle);
    if(rc != success){
      return RBFM_OPEN_FAILED;
    }

    vector<Attribute> tableDescriptor;
    createTableDescriptor(tableDescriptor);

    void *tuple = malloc(200);
    const string fileName = tableName + ".tbl";
    prepareTables(tableCounter++, tableName, fileName, tuple, tableDescriptor);
    
    RID rid;
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, tuple, rid);







    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  RC rc;

  string filename;
  unsigned tableID;

  rc = getTableIDAndFilename(tableName, filename, tableID);
  if(rc != success) {
    return rc;
  }
  cout << tableID << endl;
  assembleAttributes(tableID);

  return success;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
/*
    // Checking to see if table exists.
    void* tableID = malloc(INT_SIZE);
    rc = getTableID(tableName, tableID);
    if(rc != success){
      return -1;
    }

    // string FileName = file-name found in the catalog matching the tableID.-----------------
    // recordDescriptor is gotten by assembleAttributes
    fileHandle FileHandle;
    recordDescriptor RecordDescriptor;

    _rbf_manager->openFile(FileName, FileHandle);

    _rbf_manager->insertRecord(FileHandle, RecordDescriptor, data, RID);
    _rbf_manager->closeFile(FileHandle);
*/
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
/*
    // Checking to see if table exists.
    void* tableID = malloc(INT_SIZE);
    rc = getTableID(tableName, tableID);
    if(rc != success){
      return -1;
    }

    // Checking to see if a tuple with the RID exists.

    // string FileName = file-name found in the catalog matching the tableID.-----------------
    // recordDescriptor is gotten by assembleAttributes

    fileHandle FileHandle;
    recordDescriptor RecordDescriptor;

    _rbf_manager->openFile(FileName, FileHandle);

    _rbf_manager->deleteRecord(FileHandle, RecordDescriptor, data, RID);
    _rbf_manager->closeFile(FileHandle);
*/
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
/*
    // Checking to see if table exists.
    void* tableID = malloc(INT_SIZE);
    rc = getTableID(tableName, tableID);
    if(rc != success){
      return -1;
    }

    // Checking to see if a tuple with the RID exists.

    // string FileName = file-name found in the catalog matching the tableID.-----------------
    // recordDescriptor is gotten by assembleAttributes

    fileHandle FileHandle;
    recordDescriptor RecordDescriptor;

    _rbf_manager->openFile(FileName, FileHandle);

    _rbf_manager->readRecord(FileHandle, RecordDescriptor, RID, data);
    _rbf_manager->closeFile(FileHandle);
*/
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    _rbf_manager->printRecord(attrs, data);
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
/*
    // Checking to see if table exists.
    void* tableID = malloc(INT_SIZE);
    rc = getTableID(tableName, tableID);
    if(rc != success){
      return -1;
    }

    // Checking to see if a tuple with the RID exists.

    // string FileName = file-name found in the catalog matching the tableID.-----------------
    // recordDescriptor is gotten by assembleAttributes

    fileHandle FileHandle;
    recordDescriptor RecordDescriptor;

    _rbf_manager->openFile(FileName, FileHandle);
    _rbf_manager->readAttribute(FileHandle, RecordDescriptor, RID, attributeName, data);
    _rbf_manager->closeFile(FileHandle);
*/
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
  string filename;
  unsigned tableID;
  getTableIDAndFilename(tableName, filename, tableID);
  FileHandle fileHandle;
  _rbf_manager->openFile(filename, fileHandle);
  vector<Attribute> recordDescriptor = assembleAttributes(tableID);

  rm_ScanIterator.scanner.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
  return success;
}

void RelationManager::createTableDescriptor(vector<Attribute> &tableDescriptor){
  // Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  Attribute attr;
  attr.name = "table-id";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  tableDescriptor.push_back(attr);

  attr.name = "table-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  tableDescriptor.push_back(attr);

  attr.name = "file-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  tableDescriptor.push_back(attr);
}

void RelationManager::createColumnDescriptor(vector<Attribute> &columnDescriptor){
  // Columns(table-id:int, column-name:varchar(50), 
  // column-type:int, column-length:int, column-position:int) 
  Attribute attr;
  attr.name = "table-id"; attr.type = TypeInt;
  attr.length = (AttrLength)4;
  columnDescriptor.push_back(attr);

  attr.name = "column-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  columnDescriptor.push_back(attr);

  attr.name = "column-type";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  columnDescriptor.push_back(attr);

  attr.name = "column-length";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  columnDescriptor.push_back(attr);

  attr.name = "column-position";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  columnDescriptor.push_back(attr);
}

void RelationManager::prepareTables(int table_id, const string &table_name, const string &file_name, void *buffer, vector<Attribute> &tableDescriptor){

  int nullAttributesIndicatorActualSize = _rbf_manager->getNullIndicatorSize(tableDescriptor.size());
  unsigned char *nullAttributesIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
  memset(nullAttributesIndicator, 0, nullAttributesIndicatorActualSize);

  int offset = 0;

  memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
  offset += nullAttributesIndicatorActualSize;

  // Beginning of the actual data    
  // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
  // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]
  int id_size = sizeof(int);
  memcpy((char*)buffer + offset, &table_id, id_size);
  offset += id_size;

  const int table_name_size = table_name.length();
  memcpy((char *)buffer + offset, &table_name_size, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, table_name.c_str(), table_name_size);
  offset += table_name_size;

  const int file_name_size =file_name.length();
  memcpy((char *)buffer + offset, &file_name_size, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, file_name.c_str(), file_name_size);
  offset += file_name_size;

}

void RelationManager::prepareColumns(int table_id, const string &column_name, int column_type, int column_length, int column_position, void *buffer, vector<Attribute> &tableDescriptor){

  int nullAttributesIndicatorActualSize = _rbf_manager->getNullIndicatorSize(tableDescriptor.size());
  unsigned char *nullAttributesIndicator = (unsigned char*) malloc(nullAttributesIndicatorActualSize);
  memset(nullAttributesIndicator, 0, nullAttributesIndicatorActualSize);

  int offset = 0;

  memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
  offset += nullAttributesIndicatorActualSize;

  unsigned int_size = sizeof(int);
  memcpy((char*)buffer + offset, &table_id, int_size);
  offset += int_size;

  const int column_name_size = column_name.length();
  memcpy((char *)buffer + offset, &column_name_size, int_size);
  offset += int_size;
  memcpy((char *)buffer + offset, column_name.c_str(), column_name_size);
  offset += column_name_size;

  memcpy((char*)buffer + offset, &column_type, int_size);
  offset += int_size;

  memcpy((char*)buffer + offset, &column_length, int_size);
  offset += int_size;

  memcpy((char*)buffer + offset, &column_position, int_size);
  offset += int_size;

}



RC RelationManager::createCatalogColumns(int tableID, int columnID){
    RC rc;
    rc = _rbf_manager->createFile(columnsFileName);
    if(rc != success){
      return rc;
    }

    FileHandle fileHandle;
    rc = _rbf_manager->openFile(columnsFileName, fileHandle);
    if(rc != success){
      return rc;
    }

    vector<Attribute> columnDescriptor;
    createColumnDescriptor(columnDescriptor);


    void *tuple = malloc(200);


    vector<void*> columnTuples;

    prepareColumns(tableID, "table-id", TypeInt, 4, 1, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(tableID, "table-name", TypeVarChar, 50, 2, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(tableID, "file-name", TypeVarChar, 50, 3, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(columnID, "table-id", TypeInt, 4, 1, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(columnID, "column-name", TypeVarChar, 50, 2, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(columnID, "column-type", TypeInt, 4, 3, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(columnID, "column-length", TypeInt, 4, 4, tuple, columnDescriptor);
    columnTuples.push_back(tuple);
    prepareColumns(columnID, "column-position", TypeInt, 4, 5, tuple, columnDescriptor);
    columnTuples.push_back(tuple);



    RID rid;
    for(unsigned i = 0; i < columnTuples.size();i++){
      rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, columnTuples[i], rid);
      if(rc != success){
        return rc;
      }
    }
    
    rc = _rbf_manager->closeFile(fileHandle);
    if(rc != success){
      return rc;
    }
    return success;
}

RC RelationManager::getTableIDAndFilename (const string tableName, string &filename, unsigned &tableId) {
    RBFM_ScanIterator scanner;

    void *conditionName = malloc(tableName.size() + VARCHAR_LENGTH_SIZE);
    *((unsigned*) conditionName) = tableName.size();
    memcpy((uint8_t*) conditionName + VARCHAR_LENGTH_SIZE, tableName.c_str(), tableName.size);

    string arr[] = {"table-id", "file-name"};
    vector<string> projectionAttributes(arr, arr + sizeof(arr) / sizeof(arr[0]));
    
    FileHandle fileHandle;
    _rbf_manager->openFile(tablesFileName, fileHandle);

    vector<Attribute> recordDescriptor;
    createTableDescriptor(recordDescriptor);

    _rbf_manager->scan(fileHandle, recordDescriptor, "table-name", EQ_OP, conditionName, projectionAttributes, scanner);

    void *returnedData = malloc(128);
    RID rid;
    if (scanner.getNextRecord(rid, returnedData) == RM_EOF) {
      cout << "getTableIdAndFilename: table '" << tableName << "' not found" << endl;
      return RM_TABLE_NOT_FOUND;
    }

    int numNullBytes = int(ceil((double) projectionAttributes.size() / CHAR_BIT));
    char *curField = (char *) returnedData + numNullBytes;
    memcpy(&tableId, curField, INT_SIZE);
    curField += INT_SIZE;

    unsigned filenameSize;
    memcpy(&filenameSize, curField, VARCHAR_LENGTH_SIZE);
    curField += filenameSize;
    char char_filename[filenameSize + 1];
    memcpy(char_filename, curField, filenameSize);
    char_filename[filenameSize] = '\0';
    filename = string(char_filename);

    free(returnedData);
    free(conditionName);
    return success;
}

RM_ScanIterator::RM_ScanIterator() {
  scanner = RBFM_ScanIterator();
}

RM_ScanIterator::~RM_ScanIterator() {

}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
  RC retval = scanner.getNextRecord(rid, data);
  return retval == RBFM_EOF ? RM_EOF : retval;
}

RC RM_ScanIterator::close() {
  scanner.close();
  delete this;
  return success;
}
