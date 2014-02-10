/* 
 * Copyright 2014, Pythia authors (see AUTHORS file).
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

#include <sys/unistd.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

/* Include platform-specific headers */
#if defined(__linux__) || defined(_LINUX_) || defined(_linux_)
#define OS_LINUX
#include <sched.h>
#endif

#if defined(_SOLARIS_) || defined(sun) || defined(__sun__)
#define OS_SOLARIS
#include <sys/processor.h>
#endif

#if !defined(OS_SOLARIS) && !defined(OS_LINUX)
#error Platform is not supported. Only Linux and Solaris platforms are supported in this code.
#endif

#include "ProcessorMap.h" 
#define fatal(...)

ProcessorMap::ProcessorMap() { 
  m_nProcs = 0;
  m_p_nProcessor_Ids = NULL;

  m_nProcs = DetermineNumberOfProcessors();
  if( m_nProcs <= 0 ) {
#ifdef OS_SOLARIS
    fatal("sysconf() reports %i processors online.\n", m_nProcs );
#endif
#ifdef OS_LINUX
    fatal("sched_getaffinity() reports empty processor mask.\n");
#endif
  }

  m_p_nProcessor_Ids = new int[m_nProcs];
  if(m_p_nProcessor_Ids == NULL ) {
    fatal("new int[%i] returned NULL -- out of memory?\n", m_nProcs );
  }

  unsigned int i;
  int n = 0;

#ifdef OS_SOLARIS
  int status;
  for(i=0;n<m_nProcs && i<4096 ;i++) {
    status = p_online(i,P_STATUS);
    if(status==-1 && errno==EINVAL) continue;
    
    m_p_nProcessor_Ids[n] = i;
    n++;
  }

#endif
#ifdef OS_LINUX
  cpu_set_t cpus;

  // Returns number of processors available to process (based on affinity mask)
  if( sched_getaffinity(0, sizeof(cpus), (cpu_set_t*) &cpus) < 0) {
    fatal("sched_getaffinity() reports empty processor mask.\n" );
  }

  for (i = 0; n<m_nProcs && i < sizeof(cpus)*8; i++) {
    if( CPU_ISSET( i, &cpus ) ) {
      m_p_nProcessor_Ids[n] = i;
      n++;
    }
  }

#endif

  if( n != m_nProcs ) {
    fatal("Unable to find all processor numbers.\n" );
  }

} 

ProcessorMap::~ProcessorMap() { 
  if( m_p_nProcessor_Ids != NULL ) {
    delete [] m_p_nProcessor_Ids;
    m_p_nProcessor_Ids = NULL;
  }
} 

int ProcessorMap::LogicalToPhysical(int lproc) const { 
  IntegrityCheck();
  if( lproc < 0 || lproc >= m_nProcs ) {
    fatal( "Logical processor number out of range: [%i,%i) (%i)",0,m_nProcs,lproc);
  }

  return m_p_nProcessor_Ids[lproc];
} 

int ProcessorMap::PhysicalToLogical(int pproc) const { 
  IntegrityCheck();

  int i;
  for(i=0;i<m_nProcs;i++) {
    if( m_p_nProcessor_Ids[i] == pproc ) break;
  }

  if( i == m_nProcs ) {
    fatal( "Physical processor number does not match any known physical processor numbers.");
  }

  return i;
} 

void ProcessorMap::IntegrityCheck( ) const {
  if( m_nProcs == 0 || m_p_nProcessor_Ids == NULL ) {
    fatal( "Processor Map not in a usable state." );
  }
} 

/* Borrowed from the Phoenix MapReduce runtime */
int ProcessorMap::DetermineNumberOfProcessors() {
   int nProcs = 0;
#ifdef OS_LINUX
   cpu_set_t cpus;
   
   // Returns number of processors available to process (based on affinity mask)
   if( sched_getaffinity(0, sizeof(cpus), (cpu_set_t*) &cpus) < 0) {
     nProcs = -1;
     CPU_ZERO( &cpus );
   }

   for (unsigned int i = 0; i < sizeof(cpus)*8; i++) {
      if( CPU_ISSET( i, &cpus )) {
        nProcs++;
      }
   }
#endif

#ifdef OS_SOLARIS
   nProcs = sysconf(_SC_NPROCESSORS_ONLN);
   if( nProcs < 0 ) {
     nProcs = -1;
   }
#endif

   return nProcs;
}

void ProcessorMap::BindToPhysicalCPU( int pproc ) const {
  /* Verify pproc is in the physical cpu array */
  int lcpu = -1;
  for( int i=0; i<m_nProcs;i++ ) {
    if( m_p_nProcessor_Ids[i] == pproc ) {
      lcpu = i;
      break;
    }
  }

  if( lcpu != -1 ) {
#ifdef OS_SOLARIS
    if( processor_bind(P_LWPID, P_MYID, pproc, NULL) < 0 ) {
      fatal("Call to processor_bind() failed for physical CPU %i\n",pproc);
    }
#endif
#ifdef OS_LINUX
    cpu_set_t myProc;
    CPU_ZERO( &myProc );
    CPU_SET( pproc, &myProc );

    if( sched_setaffinity(0, sizeof(myProc), &myProc) < 0 ) {
      fatal("Call to sched_setaffinity() failed for physical CPU %i\n",pproc);
    }
#endif
  } else {
    fatal("Failed to bind to processor %i\n -- Processor does not exist!",pproc );
  }
}

