
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

#include <cstdlib>
#include <ctime>
#include "custom_asserts.h"

void ReentrantRandom::init(unsigned int salt)
{
	/*
	 * Initialization vector lifted from glibc.
	 */
	static int32_t const glibcrandtbl[] = 
	{
		0,
		-1726662223, 379960547, 1735697613, 1040273694, 1313901226,
		1627687941, -179304937, -2073333483, 1780058412, -1989503057,
		-615974602, 344556628, 939512070, -1249116260, 1507946756,
		-812545463, 154635395, 1388815473, -1926676823, 525320961,
		-1009028674, 968117788, -123449607, 1284210865, 435012392,
		-2017506339, -911064859, -370259173, 1132637927, 1398500161,
		-205601318,
	};

	memcpy(buffer, glibcrandtbl, sizeof(glibcrandtbl));
	memset(&state, 0, sizeof(state));
	initstate_r(time(NULL) ^ (salt << 4), buffer, sizeof(glibcrandtbl), &state);
}

int ReentrantRandom::next()
{
	int randval;
	int ret;
	ret = random_r(&state, &randval);
	dbgassert(ret == 0);
	return randval;
}
