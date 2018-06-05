
#include "qe.h"
#include "string.h"

bool Iterator::fieldIsNull(void *data, int i) {
	uint8_t nullByte = ((uint8_t *) data)[i / 8];
	uint8_t mask = 128 >> (i % 8);
	return (nullByte & mask) > 0;
}

void Iterator::setFieldNull(void *data, int i) {
	((char *) data)[i / 8] |= 128 >> (i % 8);
}

unsigned Iterator::getNumNullBytes(unsigned numAttributes) {
    return int(ceil((double) numAttributes / CHAR_BIT));
}

unsigned Iterator::getFieldLength(void *field, Attribute &attr) {
	unsigned length;
	switch (attr.type) {
	case TypeInt:
	case TypeReal:
		length = attr.length;
		break;
	case TypeVarChar: {
		uint32_t varcharLength;
		memcpy(&varcharLength, field, VARCHAR_LENGTH_SIZE);
		length = VARCHAR_LENGTH_SIZE + varcharLength;
		break;
	}
	}
	return length;
}

Filter::Filter(Iterator* input, const Condition &condition)
{
	this->input = input;
	this->attrNames = attrNames;
	this->cond = condition;


	// right side is a join operator not a filter
	if(cond.bRhsIsAttr);


	input->getAttributes(inputAttrs);


	for(unsigned i = 0; i < inputAttrs.size(); i++){
		if(inputAttrs[i].name == this->cond.lhsAttr){
			this->index = i;
		}
	}


	inputTupleSize = 0;
	for (Attribute &attr: inputAttrs) {
		inputTupleSize += attr.length;
	}
}

Filter::~Filter()
{
}

RC Filter::getNextTuple(void *data)
{
	void *origData = malloc(inputTupleSize);

	bool status = false;
	while(input->getNextTuple(origData) != QE_EOF) {
		unsigned recordNullIndicatorSize = getNumNullBytes(inputAttrs.size());
		char recordNullIndicator[recordNullIndicatorSize];
		memcpy (recordNullIndicator, origData, recordNullIndicatorSize);

		// value at this position is null
		if(recordNullIndicator[index]){
			return -1;
		}

		// void *

		switch (cond.rhsValue.type)
		{
			case TypeInt:
				status = filterData(*(int *)&cond.rhsValue.data, cond.op, *(int *)&origAttribute);
			case TypeReal:
				status = filterData(*(float *)&cond.rhsValue.data, cond.op, *(float *)&origAttribute);
			case TypeVarChar:
				status = filterData(cond.rhsValue.data, cond.op, origAttribute);
			default: status = false;
		}

		if(status == true){
			memcpy(data, origData, inputTupleSize);
			return SUCCESS;
		}
	}
	return QE_EOF;
}

bool Filter::filterData(int recordInt, CompOp compOp, const int intValue)
{
    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::filterData(float recordReal, CompOp compOp, const float realValue)
{
    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::filterData(void *recordString, CompOp compOp, const void *value)
{
    // if (compOp == NO_OP)
    //     return true;

    // int32_t valueSize;
    // memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    // char valueStr[valueSize + 1];
    // valueStr[valueSize] = '\0';
    // memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    // int cmp = strcmp(recordString, valueStr);
    // switch (compOp)
    // {
    //     case EQ_OP: return cmp == 0;
    //     case LT_OP: return cmp <  0;
    //     case GT_OP: return cmp >  0;
    //     case LE_OP: return cmp <= 0;
    //     case GE_OP: return cmp >= 0;
    //     case NE_OP: return cmp != 0;
    //     // Should never happen
    //     default: return false;
    // }
	return false;
}
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for (string attrName: attrNames) {
		auto pred = [&](Attribute attr) { return attr.name == attrName; };
		vector<Attribute>::const_iterator attr = find_if(inputAttrs.begin(), inputAttrs.end(), pred);
		attrs.push_back(*attr);
	}
}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->input = input;
	this->attrNames = attrNames;
	input->getAttributes(inputAttrs);
	inputTupleSize = 0;
	for (Attribute &attr: inputAttrs) {
		inputTupleSize += attr.length;
	}
}

Project::~Project()
{
}

RC Project::getNextTuple(void *data)
{
	void *origData = malloc(inputTupleSize);
	if (input->getNextTuple(origData) != QE_EOF) {
		if(projectAttributes(origData, data))
			return QE_EOF;
		return SUCCESS;
	}
	return QE_EOF;
}

RC Project::projectAttributes(void *origData, void *newData) {
	unsigned origNumNullBytes = getNumNullBytes(inputAttrs.size());
	unsigned newNumNullBytes = getNumNullBytes(attrNames.size());

	// set all null bytes in newData to 0
	memset(newData, 0, newNumNullBytes);

	// offset into newData
	// skip null bytes
	unsigned newOffset = newNumNullBytes;
	// iterate through list of new attribute names
	for (unsigned newIndex = 0; newIndex < attrNames.size(); newIndex++) {
		// offset into origData
		unsigned origOffset = origNumNullBytes;
		// iterate through original record descriptor
		unsigned origIndex;
		for (origIndex = 0; origIndex < inputAttrs.size(); origIndex++) {
			Attribute curAttr = inputAttrs[origIndex];
			bool nullField = fieldIsNull(origData, origIndex);
			// if this entry in the record descriptor matches the current attribute name
			// AND this field in the original tuple is null
			if (curAttr.name == attrNames[newIndex] && nullField) {
				// set this field to be null in the new tuple
				setFieldNull(newData, newIndex);
				// break from inner loop
				break;
			}
			// if this entry in the record descriptor matches the current attribute name
			// AND this field in the original tuple is NOT null
			else if (curAttr.name == attrNames[newIndex] && !nullField) {
				// copy this field from the old tuple to the new one
				unsigned fieldSize = getFieldLength((char *) origData + origOffset, curAttr);
				memcpy((char *) newData + newOffset, (char *) origData + origOffset, fieldSize);
				newOffset += fieldSize;
				// break from inner loop
				break;
			}
			// if neither of these conditions are met and this field isn't null, advance origOffset and loop
			if (!nullField) {
				unsigned fieldLength = getFieldLength((char *) origData + origOffset, curAttr);
				origOffset += fieldLength;
			}
		}
		if (origIndex == inputAttrs.size())
			return QE_ATTR_NOT_FOUND;
	}

	return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for (string attrName: attrNames) {
		auto pred = [&](Attribute attr) { return attr.name == attrName; };
		vector<Attribute>::const_iterator attr = find_if(inputAttrs.begin(), inputAttrs.end(), pred);
		attrs.push_back(*attr);
	}
}

INLJoin::INLJoin(Iterator *leftIn,
	  IndexScan *rightIn,
	  const Condition &condition)
{
	// Verify that the condition contains two attributes (it is a JOIN).
	if(!condition.bRhsIsAttr);

	// Check if right attribute matches condition.
	if(rightIn->attrName == condition.rhsAttr);

	this->leftIn = leftIn;
	this->rightIn = rightIn;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	this->cond = condition;
	inputTupleSize = 0;

	for(Attribute attr: leftAttrs) {
		inputTupleSize += attr.length;
	}

	// Check if all attributes are the same. If so, they're the same table,
	// and we want to rename the inner one to clarify which is which.
	bool same = true;
	if(leftAttrs.size() == rightAttrs.size()){

		for(unsigned i = 0; i < leftAttrs.size(); i++){
			if(leftAttrs[i].name != rightAttrs[i].name){
				same = false;
				break;
			}
		}
	} else same = false;
	if(same) rightIn->tableName += "2";
}

INLJoin::~INLJoin()
{
}

RC INLJoin::getNextTuple(void *data)
{
	void *origData = malloc(inputTupleSize);
	IndexScan *riCopy = rightIn;

	if(leftIn->getNextTuple(origData) != QE_EOF) {

		// If we are not one checking every tuple in the rightIn,
		// we continue scanning through that.
		if(rightIn->getNextTuple(origData) != QE_EOF)

			if(join(origData, data))
				return QE_EOF;

		// Otherwise, we reset rightIn for the next tuple to be
		// scanned.
		riCopy = new IndexScan(rightIn->rm, rightIn->tableName,
									rightIn->attrName);
	}
	return SUCCESS;
}

RC INLJoin::join(void *origData, void *newData){

	return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	// Setting attrs to all attributes represented in inner and outer tables.
	attrs.clear();
	attrs = this->leftAttrs;
	attrs.insert(attrs.end(), (rightIn->attrs).begin(), (rightIn->attrs).end());

}