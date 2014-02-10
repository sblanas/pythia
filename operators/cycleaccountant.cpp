/*
 * Copyright 2011, Pythia authors (see AUTHORS file).
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

#include "../rdtsc.h"
#include "../util/numaallocate.h"

void CycleAccountant::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);

	schema = nextOp->getOutSchema();

	for (int i=0; i<MAX_THREADS; ++i) {
		cycles.push_back(NULL);
	}
}

void CycleAccountant::threadInit(unsigned short threadid)
{
	cycles.at(threadid) = (CyclesPerOp*) 
		numaallocate_local("CycA", sizeof(CyclesPerOp), this);
}

Operator::ResultCode CycleAccountant::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode res;
	cycles[threadid]->ScanStartCycles -= curtick();
	res = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	cycles[threadid]->ScanStartCycles += curtick();
	return res;
}

Operator::GetNextResultT CycleAccountant::getNext(unsigned short threadid)
{
	GetNextResultT res;
	cycles[threadid]->GetNextCycles -= curtick();
	res = nextOp->getNext(threadid);
	cycles[threadid]->GetNextCycles += curtick();
	return res;
}

Operator::ResultCode CycleAccountant::scanStop(unsigned short threadid)
{
	ResultCode res;
	cycles[threadid]->ScanStopCycles -= curtick();
	res = nextOp->scanStop(threadid);
	cycles[threadid]->ScanStopCycles += curtick();
	return res;
}

void CycleAccountant::threadClose(unsigned short threadid)
{
	numadeallocate(cycles.at(threadid));
}

