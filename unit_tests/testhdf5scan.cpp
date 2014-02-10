
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

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"
#include "../util/numaasserts.h"

#include "common.h"

void wipestage(void* staging, const unsigned int maxsize)
{
	memset(staging, 0xBF, maxsize);
}

template<typename T>
void verifynooverflow(void* staging, const unsigned int maxsize, 
	const unsigned int tuples)
{
	size_t goodpart = sizeof(T) * tuples;
	assert(maxsize > goodpart);
	char* arr = (char*) staging;
	for (size_t i=goodpart; i<maxsize; ++i)
	{
		char c = 0xBF;
		assert(arr[i] == c);
	}
}

void test(int retries)
{
	hid_t hdf5file;
	vector<hid_t> hdf5sets;
	vector<hid_t> hdf5space;
	hid_t memspace;

	Operator::Page* output;
	void* staging;

	unsigned long long totaltuples;
	unsigned long long sizeintup;

	string filename;
	vector<string> datasetnames;

	unsigned long long origoffset;
	unsigned long long nexttuple;

	unsigned int buffsize;
	Schema schema;

	// >>>>> init
	//
	buffsize = 256;
	origoffset = 0;
	filename = "unit_tests/data/ptf_small.h5";
	datasetnames.push_back("/candidate/id");
	datasetnames.push_back("/candidate/sub_id");
	schema.add(CT_LONG);
	schema.add(CT_LONG);
	unsigned int size = datasetnames.size();
	
	// Open file, open datasets.
	//
	hdf5file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	for (unsigned int i=0; i<size; ++i)
	{
		hdf5sets.push_back(H5Dopen2(hdf5file, datasetnames[i].c_str(), H5P_DEFAULT));
		hdf5space.push_back(H5Dget_space(hdf5sets[i]));
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

	// Repeat: threadInit, scanStart, getNext, scanStop, threadClose
	// 
	for (int attempt=0; attempt<retries; ++attempt)
	{
		// >>>>> threadInit
		//
		void* space = numaallocate_local("HdfO", sizeof(Operator::Page), NULL);
		output = new(space) Operator::Page(buffsize, schema.getTupleSize(), NULL);
		assertaddresslocal(output);

		const unsigned int totalspace = 2 * sizeintup * sizeof(CtLong);

		staging = numaallocate_local("HdfS", totalspace, NULL);
		assertaddresslocal(staging);

		// >>>>> scanStart
		//
		nexttuple = origoffset;
		sizeintup = buffsize/schema.getTupleSize();

		// Reset memspace to full size, if it has shrunk from a past scan.
		//
		hsize_t zero = 0;
		H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);

		// >>>>> getNext
		//
		do
		{
			output->clear();

			unsigned long long remaintups = (totaltuples+origoffset)-nexttuple;
			if (sizeintup > remaintups)
			{
				// Less than sizeintup tuples remaining.
				// Change sizeintup to reflect this, and shrink memory space.
				//
				sizeintup = remaintups;
				if (sizeintup == 0)
				{
					break;
				}
				hsize_t zero = 0;
				H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);
			}

			void* target = output->allocate(sizeintup * schema.getTupleSize());
			dbgassert(target != NULL);

			wipestage(staging, totalspace);

			for (unsigned int i=0; i<hdf5sets.size(); ++i)
			{
				// Read column, verify staging area has not overflowed
				//
				H5Sselect_hyperslab(hdf5space[i], H5S_SELECT_SET, 
						&nexttuple, NULL, &sizeintup, NULL);
				H5Dread(hdf5sets[i], H5T_NATIVE_LLONG, memspace, 
						hdf5space[i], H5P_DEFAULT, staging);
				verifynooverflow<CtLong>(staging, totalspace, sizeintup);
				wipestage(staging, totalspace);
			}

			nexttuple += sizeintup;

		} while (nexttuple != totaltuples);

		// >>>>> scanStop
		//
		nexttuple = totaltuples;
		sizeintup = buffsize/schema.getTupleSize();

		// >>>>> threadClose
		//
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

	// >>>>> destroy
	//
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

int main()
{
	test(1);
	test(2);
	test(10);
}

