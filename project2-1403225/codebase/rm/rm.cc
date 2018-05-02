#include "rm.h"

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
      check if these tables exist in the file
    */ 
    RC rc;
    rc = _rbf_manager->createFile(tablesFileName);
    if(rc != success){
      return RBFM_CREATE_FAILED;
    }

    FileHandle fileHandle;
    rc = _rbf_manager->openFile(tablesFileName, fileHandle);
    if(rc != success){
      return RBFM_OPEN_FAILED;
    }

    vector<Attribute> tableDescriptor;
    createTableDescriptor(tableDescriptor);


    void *tuple = malloc(200);
<<<<<<< HEAD
    prepareTables(tableCounter++, "Tables", "Tables.tbl", tuple, &tupleSize, tableDescriptor);
    RID tablesRID;
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, tuple, tablesRID);

    prepareTables(tableCounter++, "Columns", "Columns.tbl", tuple, &tupleSize, tableDescriptor);
=======
    prepareTable(tableCounter++, "Tables", "Tables.tbl", tuple, tableDescriptor);
    RID tablesRID;
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, tuple, tablesRID);







    prepareTable(tableCounter++, "Columns", "Columns.tbl", tuple, tableDescriptor);
>>>>>>> f0d9bfe2a649a995336e8c88073dd575181291bf
    RID columnsRID;
    _rbf_manager->insertRecord(fileHandle, tableDescriptor, tuple, columnsRID);


    return -1;
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
    prepareTable(tableCounter++, tableName, fileName, tuple, tableDescriptor);
    
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
    return -1;
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

  int nullAttributesIndicatorActualSize = _rbf_manager->getNullIndicatorSize(tableDescriptor.size());
  unsigned char *nullAttributesIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
  memset(nullAttributesIndicator, 0, nullAttributesIndicatorActualSize);

  int offset = 0;

  memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
  offset += nullAttributesIndicatorActualSize;

  // Beginning of the actual data    
  // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
  // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]
  int int_size = sizeof(int);
  int id_size = int_size;
  memcpy((char*)buffer + offset, &table_id, id_size);
  offset += id_size;

  const int table_name_size = sizeof(table_name);
  memcpy((char *)buffer + offset, &table_name_size, int_size);
  offset += int_size;
  memcpy((char *)buffer + offset, table_name.c_str(), table_name_size);
  offset += table_name_size;

  const int file_name_size = sizeof(file_name);
  memcpy((char *)buffer + offset, &file_name_size, int_size);
  offset += int_size;
  memcpy((char *)buffer + offset, file_name.c_str(), file_name_size);
  offset += file_name_size;

}

void prepareColumns(int table_id, const string &column_name, int column_type, int column_length, int column_position, void *buffer, vector<Attribute> &tableDescriptor){

  int nullAttributesIndicatorActualSize = _rbf_manager->getNullIndicatorSize(tableDescriptor.size());
  unsigned char *nullAttributesIndicator = (unsigned char*) malloc(nullAttributesIndicatorActualSize);
  memset(nullAttributesIndicator, 0, nullAttributesIndicatorActualSize);

  int offset = 0;

  memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
  offset += nullAttributesIndicatorActualSize;

  unsigned int_size = sizeof(int);
  memcpy((char*)buffer + offset, &table_id, int_size);
  offset += int_size;

  const int column_name_size = sizeof(column_name);
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
