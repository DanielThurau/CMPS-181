#include "rm.h"
#include <cstdio>
#include <math.h>

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
    /*
    TODO
      check if these tables exist in the file
    */ 
    RC rc;
    rc = createCatalogTables();
    if(rc != success){
      return rc;
    }


    rc = createCatalogColumns();
    if(rc != success){
      return rc;
    }




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

  // get the ID of the table we need to getAttib from
  unsigned tableID;

  // passing the table and reference to our var, returns a tableID
  rc = getTableID(tableName, tableID);
  if(rc != success){
    return rc;
  }
  // assemble a vector of attributes
  assembleAttributes(tableID);

  




  return success;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
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
  attr.name = "table-id";
  attr.type = TypeInt;
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

  int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(tableDescriptor.size());
  unsigned char *nullAttributesIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
  memset(nullAttributesIndicator, 0, nullAttributesIndicatorActualSize);

  int offset = 0;

  memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
  offset += nullAttributesIndicatorActualSize;

  // Beginning of the actual data    
  // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
  // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]
  memcpy((char*)buffer + offset, &table_id, INT_SIZE);
  offset += INT_SIZE;

  const int table_name_size = table_name.length();
  memcpy((char *)buffer + offset, &table_name_size, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy((char *)buffer + offset, table_name.c_str(), table_name_size);
  offset += table_name_size;

  const int file_name_size =file_name.length();
  memcpy((char *)buffer + offset, &file_name_size, INT_SIZE);
  offset += INT_SIZE;
  memcpy((char *)buffer + offset, file_name.c_str(), file_name_size);
  offset += file_name_size;

}

void RelationManager::prepareColumns(int table_id, const string &column_name, int column_type, int column_length, int column_position, void *buffer, vector<Attribute> &tableDescriptor){

  int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(tableDescriptor.size());
  unsigned char *nullAttributesIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize); // 8 bits with p/d sizeof()
  memset(nullAttributesIndicator, 0, nullAttributesIndicatorActualSize);

  int offset = 0;

  memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
  offset += nullAttributesIndicatorActualSize;

  memcpy((char*)buffer + offset, &table_id, INT_SIZE);
  offset += INT_SIZE;

  const int column_name_size = column_name.length();
  memcpy((char *)buffer + offset, &column_name_size, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy((char *)buffer + offset, column_name.c_str(), column_name_size);
  offset += column_name_size;

  memcpy((char*)buffer + offset, &column_type, INT_SIZE);
  offset += INT_SIZE;

  memcpy((char*)buffer + offset, &column_length, INT_SIZE);
  offset += INT_SIZE;

  memcpy((char*)buffer + offset, &column_position, INT_SIZE);
  offset += INT_SIZE;
}

RC RelationManager::createCatalogTables(){
    RC rc;
    rc = _rbf_manager->createFile(tablesFileName);
    if(rc != success){
      return rc;
    }

    // open the file with filehandle
    FileHandle fileHandle;
    rc = _rbf_manager->openFile(tablesFileName, fileHandle);
    if(rc != success){
      return rc;
    }

    // descriptor of all records in Tables.tbl
    vector<Attribute> tableDescriptor;
    createTableDescriptor(tableDescriptor);


    void *buffer = malloc(200);
    RID rid;

    cout << "\nInserting system catalouge tables" << endl;


    int tableID = tableCounter++;
    prepareTables(tableID, "Tables", "Tables.tbl", buffer, tableDescriptor);
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, buffer, rid);

    // TODO STODUT CHECK
    _rbf_manager->readRecord(fileHandle, tableDescriptor, rid, buffer);
    _rbf_manager->printRecord(tableDescriptor, buffer);
    // END TODO

    int columnID = tableCounter++;
    prepareTables(columnID, "Columns", "Columns.tbl", buffer, tableDescriptor);
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, buffer, rid);
    
    // TODO STODUT CHECK
    _rbf_manager->readRecord(fileHandle, tableDescriptor, rid, buffer);
    _rbf_manager->printRecord(tableDescriptor, buffer);
    // END TODO

    rc = _rbf_manager->closeFile(fileHandle);
    if(rc != success){
      return rc;
    }
    return success;

}





RC RelationManager::createCatalogColumns(){
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
    RID rid;

    cout << "\nInserting system catalouge columns" << endl;
    
    // TODO
    // redo so its not hardcoded
    // remove read and print, its verified 
    prepareColumns(1, "table-id", TypeInt, 4, 1, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(1, "table-name", TypeVarChar, 50, 2, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(1, "file-name", TypeVarChar, 50, 3, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(2, "table-id", TypeInt, 4, 1, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(2, "column-name", TypeVarChar, 50, 2, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(2, "column-type", TypeInt, 4, 3, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(2, "column-length", TypeInt, 4, 4, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);
    prepareColumns(2, "column-position", TypeInt, 4, 5, tuple, columnDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, columnDescriptor, tuple, rid);
    rc = _rbf_manager->readRecord(fileHandle, columnDescriptor, rid, tuple);
    _rbf_manager->printRecord(columnDescriptor, tuple);

    
    rc = _rbf_manager->closeFile(fileHandle);
    if(rc != success){
      return rc;
    }
    return success;
}



RC RelationManager::getTableID(const string &tableName, unsigned &data){

    // 
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tablesFileName, fileHandle);
    if(rc != success){
      return rc;
    }

    vector<Attribute> recordDescriptor;
    createTableDescriptor(recordDescriptor);

    RBFM_ScanIterator scanner;


    void *buffer = malloc(50);
    int offset = 0;

    string conditionalValue = tableName;
    const unsigned conditionalValueSize = conditionalValue.length();
    memcpy((char *)buffer + offset, &conditionalValueSize, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, conditionalValue.c_str(), conditionalValueSize);





    string arr[] = {"table-id"};
    vector<string> projectionAttributes(arr, arr + sizeof(arr) / sizeof(arr[0]));
    
    _rbf_manager->scan(fileHandle, recordDescriptor, "table-name", EQ_OP, buffer, projectionAttributes, scanner);


    RID returnedRID;
    void *returned_data = malloc(4);

    if(scanner.getNextRecord(returnedRID, returned_data) == RBFM_EOF){
      return -1;
    }
    // magic number do not fuck with
    offset = getActualByteForNullsIndicator(3);
    memcpy(&data, ((char*) returned_data + offset), INT_SIZE);
    cout << data << endl;
    rc = _rbf_manager->closeFile(fileHandle);
    if(rc != success){
      return rc;
    }

    return success;
}


vector<Attribute> RelationManager::assembleAttributes(unsigned tableID){
    // set of attributes to be returned
    vector<Attribute> attributes;
    // file handle for the columnTables
    FileHandle fileHandle;
    _rbf_manager->openFile(columnsFileName, fileHandle);

    // creates a descriptor for a column table entry
    vector<Attribute> columnDescriptor;
    createColumnDescriptor(columnDescriptor);

    // initate the scanner and create the projetion fields
    RBFM_ScanIterator scanner;
    string arr[] = {"column-name", "column-type", "column-length", "column-position"};
    vector<string> projectionAttributes(arr, arr + sizeof(arr) / sizeof(arr[0]));

    // assembleAttributs match on the tableID
    _rbf_manager->scan(fileHandle, columnDescriptor, "table-id", EQ_OP, &tableID, projectionAttributes, scanner);

    
    

    // rid returned from getNextRecord
    RID rid;
    // attribute to be populated by a 
    Attribute attribute;
    void *buffer = malloc(500);


    while (scanner.getNextRecord(rid, buffer) != RBFM_EOF) {
      int index = columnEntry(buffer, attribute, projectionAttributes) - 1;
      if((unsigned)index == attributes.size()){
        unsigned extend = (unsigned)index - attributes.size();
        attributes.resize(extend);
      }
      attributes.insert(attributes.begin() + index - 1, attribute);
    }


    _rbf_manager->closeFile(fileHandle);

    return attributes;
}

int RelationManager::columnEntry(void *columnRecord, Attribute &entry, vector<string> projectionAttributes){
    int offset = getActualByteForNullsIndicator(projectionAttributes.size());

    /*
      struct Attribute {
        string   name;     // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
      };
    */


    uint32_t varcharSize;
    memcpy(&varcharSize, ((char*) columnRecord + offset), VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    char buffer[varcharSize + 1];


    for (unsigned i = 0; i < varcharSize; i++) {
        buffer[i] = ((char*)columnRecord + offset)[i];
        
    }
    offset+=varcharSize;
    buffer[varcharSize] = '\0';
    entry.name = string(buffer);
    
    memcpy(&entry.type, ((char*) columnRecord + offset), INT_SIZE);
    offset += INT_SIZE;

    memcpy(&entry.length, ((char*) columnRecord + offset), INT_SIZE);
    offset += INT_SIZE;

    int column_position;
    memcpy(&column_position, ((char*) columnRecord + offset), INT_SIZE);

    return column_position;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RelationManager::getActualByteForNullsIndicator(int fieldCount) {
  return ceil((double) fieldCount / CHAR_BIT);
}

