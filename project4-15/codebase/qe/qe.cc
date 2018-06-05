
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

bool Iterator::recordDescriptorsEqual(vector<Attribute> &rd_1, vector<Attribute> &rd_2) {
	if (rd_1.size() != rd_2.size())
		return false;
	
	return equal(rd_1.begin(), rd_1.end(), rd_2.begin(), rd_2.end());
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
			compType = inputAttrs[i].type;
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
	void *origAttr= malloc(compType);

	bool status = false;
	while(input->getNextTuple(origData) != QE_EOF) {
		unsigned nullIndicatorSize = getNumNullBytes(inputAttrs.size());

		// value at this position is null
		if(fieldIsNull(origData, index)){
			if(cond.op == NO_OP){
				memcpy(data, origData, inputTupleSize);
				return SUCCESS;
			}
		}

		unsigned offset = sizeof(nullIndicatorSize);
		// advance data through fields until we reach index
		for (unsigned i = 0; i < index; i++) {
			switch (inputAttrs[i].type) {
			case TypeInt:
			case TypeReal:
				offset += INT_SIZE;
				break;
			case TypeVarChar:
				uint32_t varchar_length;
				memcpy(&varchar_length, data, VARCHAR_LENGTH_SIZE);
				offset += VARCHAR_LENGTH_SIZE + varchar_length;
				break;
			}
		}

		switch (cond.rhsValue.type)
		{
			case TypeInt:
				status = filterData(*(uint32_t *)cond.rhsValue.data, cond.op, *(uint32_t *)origAttr);
			case TypeReal:
				status = filterData(*(float *)cond.rhsValue.data, cond.op, *(float *)origAttr);
			case TypeVarChar:
				status = filterData(cond.rhsValue.data, cond.op, origAttr);
			default: status = false;
		}

		if(status == true){
			memcpy(data, origData, inputTupleSize);
			return SUCCESS;
		}
	}
	return QE_EOF;
}

bool Filter::filterData(uint32_t recordInt, CompOp compOp, const uint32_t intValue)
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
	CartProd *cartProd = new CartProd(leftIn, rightIn);
	cartProd->getAttributes(attrs);

	filter = new Filter(cartProd, condition);

	// Check if all attributes are the same. If so, they're the same table,
	// and we want to rename the inner one to clarify which is which.
	vector<Attribute> leftAttrs;
	leftIn->getAttributes(leftAttrs);
	vector<Attribute> rightAttrs;
	rightIn->getAttributes(rightAttrs);
	bool same = recordDescriptorsEqual(leftAttrs, rightAttrs);

	if(same) rightIn->tableName += "2";
}

INLJoin::~INLJoin()
{
}

RC INLJoin::getNextTuple(void *data)
{
	return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	copy(this->attrs.begin(), this->attrs.end(), attrs.begin());
}

// Pretty much assumes all properties of the INLJoin that calls it (except the
// Condition) and iterates through the inner table for every tuple in the outer
CartProd::CartProd(Iterator *leftIn, IndexScan *rightIn)
{
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	leftInputTupleSize = 0;
	for(Attribute attr : leftAttrs){
		leftInputTupleSize += attr.length;
	}

	rightInputTupleSize = 0;
	for(Attribute attr : rightAttrs){
		rightInputTupleSize += attr.length;
	}

	leftData = malloc(leftInputTupleSize);
	leftIn->getNextTuple(leftData);
}

CartProd::~CartProd()
{
	free(leftData);
}

RC CartProd::getNextTuple(void *data)
{
	void *rightData = malloc(rightInputTupleSize);

	if(rightIn->getNextTuple(rightData) == QE_EOF){

		if(leftIn->getNextTuple(leftData) == QE_EOF){
			free(rightData);
			return QE_EOF;
		}

		if (rightIn->getNextTuple(rightData) == QE_EOF) {
			free(rightData);
			return QE_EOF;
		}
		rightIn->setIterator(NULL, NULL, true, true);
	}

	memcpy(data, leftData, leftInputTupleSize);
	memcpy((char *) data + leftInputTupleSize,
		rightData, rightInputTupleSize);

	free(rightData);
	return SUCCESS;
}


void CartProd::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	copy(leftAttrs.begin(), leftAttrs.end(), attrs.begin());
	attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());
}