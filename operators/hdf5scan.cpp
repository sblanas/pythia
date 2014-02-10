
/*
 * Copyright 2013, Pythia authors (see AUTHORS file).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "operators.h"
#include "operators_priv.h"

#include "../util/numaallocate.h"
#include "../util/numaasserts.h"

using std::make_pair;

/**
 * Appends column in \a output schema based on given HDF5 dataset. 
 * The caller is responsible for opening and closing the dataset.
 */
void appendFromDataset(hid_t dataset, Schema& output)
{
	// Find out data type.
	//
	hid_t datatype = H5Dget_type(dataset);
	H5T_class_t classid = H5Tget_class(datatype);

	// Find if datatype class is supported.
	//
	switch (classid)
	{
		// If string, return now.
		//
		case H5T_STRING:
			output.add(CT_CHAR, H5Tget_size(datatype));
			break;

		// If numeric, find if the particular data type is supported.
		//
		{
		case H5T_INTEGER:
		case H5T_FLOAT:
			vector<hid_t> all;
			all.resize(6);
			all[0] = H5Tcopy(H5T_NATIVE_SHORT);
			all[1] = H5Tcopy(H5T_NATIVE_INT);
			all[2] = H5Tcopy(H5T_NATIVE_LLONG);
			all[3] = H5Tcopy(H5T_NATIVE_FLOAT);
			all[4] = H5Tcopy(H5T_NATIVE_DOUBLE);
			all[5] = H5Tcopy(H5T_NATIVE_CHAR);

			hid_t nativetype = H5Tget_native_type(datatype, H5T_DIR_DEFAULT);

			if (H5Tequal(nativetype, all[0]) > 0)
				output.add(CT_INTEGER);
			else if (H5Tequal(nativetype, all[1]) > 0)
				output.add(CT_INTEGER);
			else if (H5Tequal(nativetype, all[2]) > 0)
				output.add(CT_LONG);
			else if (H5Tequal(nativetype, all[3]) > 0)
				output.add(CT_DECIMAL);
			else if (H5Tequal(nativetype, all[4]) > 0)
				output.add(CT_DECIMAL);
			else if (H5Tequal(nativetype, all[5]) > 0)
				output.add(CT_CHAR, 2);
			else
				throw IllegalConversionException();

			H5Tclose(nativetype);
			for (unsigned int i=0; i<all.size(); ++i)
			{
				H5Tclose(all[i]);
			}
			break;
		}

		// No idea what to do with other class types, throw exception.
		//
		default:
			throw IllegalConversionException();
	}

	H5Tclose(datatype);
}

void ScanHdf5Op::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	ZeroInputOp::init(root, cfg);

	filename = (const char*) root.getRoot()["path"];
	filename += "/";
	filename += (const char*) cfg["file"];

	// Remember partition id and total partitions.
	//
	int pid = 0;
	cfg.lookupValue("thispartition", pid);
	int ptotal = 1;
	cfg.lookupValue("totalpartitions", ptotal);

	assert(ptotal > 0);
	assert(pid < ptotal);
	thispartition = pid;
	totalpartitions = ptotal;

	// Store dataset names.
	//
	libconfig::Setting& grp = cfg["pick"];
	unsigned int size = grp.getLength();

	for (unsigned int i=0; i<size; ++i)
	{
		string n = grp[i];
		datasetnames.push_back(n);
	}

	// Open file, open datasets.
	//
	hdf5file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	for (unsigned int i=0; i<size; ++i)
	{
		hdf5sets.push_back(H5Dopen2(hdf5file, datasetnames[i].c_str(), H5P_DEFAULT));
		hdf5space.push_back(H5Dget_space(hdf5sets[i]));
	}

	// Create schema from datasets, and check that types are supported.
	//
	for (unsigned int i=0; i<size; ++i)
	{
		appendFromDataset(hdf5sets[i], schema);
	}

	// Assert all datasets are vectors, not arrays.
	// 
	hid_t space;
	for (unsigned int i=0; i<size; ++i)
	{
		space = H5Dget_space(hdf5sets[i]);
		assert(H5Sget_simple_extent_ndims(space) == 1);
		H5Sclose(space);
	}

	// Assert all datasets have same length.
	// Remember data size.
	//
	hsize_t length;

	assert(size != 0);
	space = H5Dget_space(hdf5sets[0]);
	H5Sget_simple_extent_dims(space, &length, NULL);
	H5Sclose(space);

	totaltuples = length;
	for (unsigned int i=1; i<size; ++i)
	{
		space = H5Dget_space(hdf5sets[i]);
		H5Sget_simple_extent_dims(space, &length, NULL);
		H5Sclose(space);
		assert(totaltuples == length);
	}

	assert(hdf5sets.size() == hdf5space.size());

	sizeintup = buffsize/schema.getTupleSize();
	memspace = H5Screate_simple(1, &sizeintup, NULL);

	// Specify totaltuples for requested partition.
	//
	unsigned long long step = totaltuples/totalpartitions;
	assert(totaltuples >= totalpartitions);
	origoffset = step * thispartition;
	totaltuples = ( thispartition == (totalpartitions - 1) )
			? totaltuples - origoffset 
			: step;
}

unsigned int ScanHdf5Op::maxOutputColumnWidth()
{
	unsigned int ret = schema.getColumnWidth(0);
	for (unsigned int i=1; i<schema.columns(); ++i)
	{
		unsigned int width = schema.getColumnWidth(i);
		if (width > ret)
			ret = width;
	}
	return ret;
}

void ScanHdf5Op::threadInit(unsigned short threadid)
{
	dbgSetSingleThreaded(threadid);
	dbgCheckSingleThreaded(threadid);

	void* space = numaallocate_local("HdfO", sizeof(Page), this);
	output = new(space) Page(buffsize, schema.getTupleSize(), this);
	assertaddresslocal(output);

	assert(sizeintup != 0);
	staging = numaallocate_local("HdfS", 
			sizeintup * maxOutputColumnWidth(), this);
	assertaddresslocal(staging);
}

Operator::ResultCode ScanHdf5Op::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgCheckSingleThreaded(threadid);

	nexttuple = origoffset;
	sizeintup = buffsize/schema.getTupleSize();

	// Reset memspace to full size, if it has shrunk from a past scan.
	//
	hsize_t zero = 0;
	H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);

	return Ready;
}

Operator::ResultCode ScanHdf5Op::scanStop(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	nexttuple = totaltuples;
	sizeintup = buffsize/schema.getTupleSize();

	return Ready;
}

void ScanHdf5Op::threadClose(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	if (output) 
	{
		numadeallocate(output);
	}
	output = NULL;

	if (staging) 
	{
		numadeallocate(staging);
	}
	staging = NULL;
}

void ScanHdf5Op::destroy()
{
	filename.clear();
	datasetnames.clear();
	totaltuples = 0;
	nexttuple = 0;
	sizeintup = 0;

	H5Sclose(memspace);
	for (unsigned int i=0; i<hdf5sets.size(); ++i)
	{
		H5Dclose(hdf5sets[i]);
		H5Sclose(hdf5space[i]);
	}
	hdf5sets.clear();
	hdf5space.clear();

	H5Fclose(hdf5file);
}

/** 
 * Copies \a nelem contiguous elements from the \a staging array into 
 * a particular \a column of the \a output buffer. The output buffer's 
 * schema is given in parameter \a schema.
 */
template <typename KeyT>
void populate(Operator::Page* output, Schema& schema, unsigned int column,
		void* staging, unsigned int nelem)
{
	// Fast path if only one column.
	//
	if (schema.columns() == 1)
	{
		assert(column==0);
		void* out = output->getTupleOffset(0);
		memcpy(out, staging, nelem * sizeof(KeyT));

		return;
	}

	// Slow path, stitch vertical layout into tuples.
	//
	dbgassert(output->getTupleOffset(nelem-1) != NULL);
	KeyT* arr = (KeyT*) staging;
	const size_t offset = (size_t) schema.calcOffset(0, column);

	for (unsigned int i=0; i<nelem; ++i)
	{
		char* tup = (char*) output->getTupleOffset(i);
		KeyT* dest = (KeyT*) (tup+offset);
		*dest = arr[i];
	}
}

void populateChar(Operator::Page* output, Schema& schema, unsigned int column,
		void* staging, unsigned int nelem)
{
	dbgassert(output->getTupleOffset(nelem-1) != NULL);
	unsigned int colwidth = schema.getColumnWidth(column);
	char* arr = (char*) staging;

	for (unsigned int i=0; i<nelem; ++i)
	{
		void* tup = output->getTupleOffset(i);
		CtChar* dest = (CtChar*) schema.calcOffset(tup, column);
		memcpy(dest, &arr[i*(colwidth-1)], colwidth-1);
		dest[colwidth] = 0;
	}
}

Operator::GetNextResultT ScanHdf5Op::getNext(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	output->clear();

	unsigned long long remaintups = (totaltuples+origoffset)-nexttuple;
	if (sizeintup > remaintups)
	{
		// Less than sizeintup tuples remaining.
		// Change sizeintup to reflect this, and shrink memory space.
		//
		sizeintup = remaintups;
		if (sizeintup == 0)
			return make_pair(Operator::Finished, output);
		hsize_t zero = 0;
		H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);
	}

	void* target = output->allocate(sizeintup * schema.getTupleSize());
	dbgassert(target != NULL);

	for (unsigned int i=0; i<hdf5sets.size(); ++i)
	{
		H5Sselect_hyperslab(hdf5space[i], H5S_SELECT_SET, 
				&nexttuple, NULL, &sizeintup, NULL);

		copySpaceIntoOutput(i);
	}
	nexttuple += sizeintup;

	if (nexttuple == totaltuples) {
		return make_pair(Operator::Finished, output);
	} else {
		return make_pair(Operator::Ready, output);
	}
}

/** 
 * Copies space at given offset i from the hdf5space array into memspace, and
 * then into output page.
 */
void ScanHdf5Op::copySpaceIntoOutput(unsigned int i)
{
	switch(schema.getColumnType(i))
	{
		case CT_INTEGER:
			H5Dread(hdf5sets[i], H5T_NATIVE_INT, memspace,
					hdf5space[i], H5P_DEFAULT, staging);
			populate<CtInt>(output, schema, i, staging, sizeintup);
			break;

		case CT_LONG:
			H5Dread(hdf5sets[i], H5T_NATIVE_LLONG, memspace, 
					hdf5space[i], H5P_DEFAULT, staging);
			populate<CtLong>(output, schema, i, staging, sizeintup);
			break;

		case CT_DECIMAL:
			H5Dread(hdf5sets[i], H5T_NATIVE_DOUBLE, memspace,
					hdf5space[i], H5P_DEFAULT, staging);
			populate<CtDecimal>(output, schema, i, staging, sizeintup);
			break;

		case CT_CHAR:
			if (schema.getColumnWidth(i) == 2)
			{
				H5Dread(hdf5sets[i], H5T_NATIVE_CHAR, memspace,
						hdf5space[i], H5P_DEFAULT, staging);
			}
			else
			{
				/* UNTESTED CODE PATH */
				H5Dread(hdf5sets[i], H5T_C_S1, memspace,
						hdf5space[i], H5P_DEFAULT, staging);
			}
			populateChar(output, schema, i, staging, sizeintup);
			break;

		default:
			throw NotYetImplemented();
	}
#ifdef DEBUG
	memset(staging, 0xbf, sizeintup * maxOutputColumnWidth());
#endif
}
