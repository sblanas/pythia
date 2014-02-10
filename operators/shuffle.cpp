
/*
 * Copyright 2010, Pythia authors (see AUTHORS file).
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

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <cstdlib>

using std::make_pair;

/*
 * TODO
 *
 * Code does partitioning before sending blocks out. There is already extensive
 * partitioning work from the sigmod'10 branch that could be brought in here.
 *
 * Make code compatible with sparc, by isolating linux-specific structures.
 *
 */

static ShuffleOp::WillisBlock* NullPage = 0;

static ShuffleOp::WillisBlock EmptyWillisBlock(0, 0, 0, 0);

#define ntohll(x) ( ( (uint64_t)(ntohl( (uint32_t)((x << 32) >> 32) )) << 32) | ntohl( ((uint32_t)(x >> 32)) ) )                                        
#define htonll(x) ntohll(x)

// Shuffle-specific exceptions.
//
class IncomingSocketSetupError{};
class OutgoingSocketSetupError{};
class ShuffleProducerPullError{};

void csvDelim(vector<char*> & list, const char* line){
    char copy[strlen(line)];
    strcpy(copy, line);
    char * curr;
    char * add;
    curr = strtok(copy, ",");
    add = new char[strlen(curr)];
    strcpy(add, curr);
#ifdef VERBOSE
    std::cout<<curr<<std::endl;
#endif
    list.push_back(add);
    while (1){
        curr = strtok(NULL, ",");
        if (curr == NULL) break;
        add = new char[strlen(curr)];
        strcpy(add, curr);
#ifdef VERBOSE
        std::cout<<curr<<std::endl;
#endif
        list.push_back(add);
    }
#ifdef VERBOSE
    std::cout<<"done delim"<<std::endl;
#endif
};

/**
 * Serialize a tuple to byte array.
 * @pre Caller must have preallocated enough memory at \a byteArray and it is
 * the same length as tuple.
 * @param byteArray Destination data address
 * @param tuple Tuple data address
 */
inline
void serialize(char* byteArray, Schema& schema, void* tuple)
{
    dbg2assert(sizeof(void*)==sizeof(long));

    for (unsigned int pos = 0 ; pos < schema.columns() ; pos += 1){
        void* s = schema.calcOffset(tuple, pos);
        void* d = schema.calcOffset(byteArray, pos);

        // copy
        switch (schema.getColumnType(pos)) 
		{
            case CT_INTEGER: {
                                 //shit, assuming unsigned int?
                                 const uint32_t* val = reinterpret_cast<const uint32_t*>(s);
                                 uint32_t fix = htonl(*val);
                                 //memcpy(d, &fix, sizeof (uint32_t));
			*reinterpret_cast<uint32_t*>(d) = fix;
                                 break;
                             }
            case CT_LONG: 
            case CT_DATE: 
            case CT_DECIMAL: 
							 {
								 dbg2assert(sizeof(CtDate)==sizeof(CtLong));
                                 const uint64_t* val = reinterpret_cast<const uint64_t*>(s);
                              uint64_t fix = htonll(*val);
                                 //memcpy(d, &fix, sizeof (uint64_t));
			*reinterpret_cast<uint64_t*>(d) = fix;
                              break;
                             }
            case CT_CHAR: {
                              const char* p = reinterpret_cast<const char*>(s);
                              char* t = reinterpret_cast<char*>(d);

                              //write p in t
                              while ( (*(t++) = *(p++)) )
                                  ;
                              break;
                          }
            case CT_POINTER: 
            default: 
						  {
                         //throw something
                         throw IllegalSchemaDeclarationException();
                     }
        }
    }
}

void ShuffleOp::destroy(){
    char* killme;
    for (unsigned int i = 0 ; i < destIPs.size() ; i += 1)
	{
        killme = destIPs.back();
        destIPs.pop_back();
        delete killme;
        close(destSockets[i]);
    }
    for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
	{
        killme = incomingIPs.back();
        incomingIPs.pop_back();
        close(incomingSockets[i]);
        delete killme;
    }
    delete myIP;
    delete incomingSockets;
    hashfn.destroy();
};

int CreateTCPServerSocket(unsigned short port)
{
    int sock;                        /* socket to create */
    struct sockaddr_in echoServAddr; /* Local address */

    /* Create socket for incoming connections */
    if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
        throw new IncomingSocketSetupError();
    }

    /* Construct local address structure */
    memset(&echoServAddr, 0, sizeof(echoServAddr));   /* Zero out structure */
    echoServAddr.sin_family = AF_INET;                /* Internet address family */
    echoServAddr.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
    echoServAddr.sin_port = htons(port);              /* Local port */

    /* Bind to the local address */
    if (bind(sock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr)) < 0){
        throw new IncomingSocketSetupError();
    }

    /* Mark the socket so it will listen for incoming connections */
    if (listen(sock, 20) < 0){
        throw new IncomingSocketSetupError();
    }

    return sock;
};

struct initConnArgs{
    int numDestIPs;
    int* destPorts;
    int* destSockets;
    vector<char*> * destIPs;
};

struct prodArgs{
    ShuffleOp* me;
    unsigned short threadid;
};

bool send(int outboundSock, char* data, int datalength){

    int flag = send(outboundSock, data, datalength, 0); 
    if (flag == -1) {
        return false;
    }   
    if (flag != datalength){
        return false;
    }   

    return true;
};


void ShuffleOp::produce(unsigned short threadid){

    Page* in;
    Operator::ResultCode rc;
    unsigned int tupoffset;
    Operator::GetNextResultT result;

#ifdef VERBOSE
    std::cout<<"threadid "<<threadid<<std::endl;
#endif

    Page* out = output[threadid];
    out->clear();

    // Recover state information and start.
    // 
    in = state[threadid].input;
    rc = state[threadid].prevresult;
    tupoffset = state[threadid].prevoffset;
#ifdef DEBUG
    assert(rc != Error);
    assert(in != NULL);
    assert(tupoffset >= 0);
    assert(tupoffset <= (buffsize / schema.getTupleSize()) + 1);
#endif

    void* tuple;
    unsigned int hashbucket;
    char c[schema.getTupleSize()];
    WillisBlock * temp;

    int totaltups = 0;
    int tupssent = 0;

    while (1) {
        while ( (tuple = in->getTupleOffset(tupoffset++)) ) {

            totaltups += 1;

            hashbucket = hashfn.hash(tuple);

            void * bucketspace = noutput[hashbucket]->allocateTuple();
			dbgassert(bucketspace != NULL);
            serialize(c, schema, tuple);
            schema.copyTuple(bucketspace, reinterpret_cast<void *> (c));

            //now full, already serialized
            if (!noutput[hashbucket]->canStoreTuple()){
                temp = noutput[hashbucket];
                noutput[hashbucket] = nsendPage;
                nsendPage = temp;

                while (!send(destSockets[hashbucket], reinterpret_cast<char*>(nsendPage->getTupleOffset(0)), buffsize)){
                }

                tupssent+= buffsize;

                noutput[hashbucket]->clear();
            }

        }

        // If input source depleted, remove state information and return.
        //
        if (rc == Finished) {
            break;
        }

        // Read more input.
        //
        result = nextOp->getNext(threadid);
        rc = result.first;
        in = result.second;
        tupoffset = 0;

        if (rc == Error) {
            throw new ShuffleProducerPullError();
        }
    }

    for (unsigned int i = 0 ; i < destIPs.size() ; i += 1)
	{
        //while the buff still has data, keep sending

        temp = noutput[i];
        noutput[i] = nsendPage;
        nsendPage = temp;

        while (!send(destSockets[i], reinterpret_cast<char*>(nsendPage->getTupleOffset(0)), nsendPage->getFill())){
        }
        tupssent+= nsendPage->getFill();

        noutput[i]->clear();
    }

    for (unsigned int i = 0 ; i < destIPs.size() ; i += 1)
	{
        close(destSockets[i]);
    }
    
    tupssent/=schema.getTupleSize();

#ifdef VERBOSE
    std::cout<<"produce got "<<totaltups<<" tuples\n";
    std::cout<<"produce sent "<<tupssent<<" tuples\n";
#endif
}

void * prodThread(void * vargs){
    pthread_detach(pthread_self());
    prodArgs * args = (prodArgs *) vargs;
    
    args->me->produce(args->threadid);

    delete args;
    return NULL;
};

void * setupConnThread(void * vargs){
    pthread_detach(pthread_self());
    initConnArgs * args = (initConnArgs *) vargs;

    int sock;                        /* Socket descriptor */
    struct sockaddr_in echoServAddr; /* Echo server address */
    unsigned short echoServPort;     /* Echo server port */

    for (int i = 0; i < args->numDestIPs ; i += 1) {

#ifdef VERBOSE
        std::cout<<"setupConnThread to "<<args->destIPs->at(i)<<std::endl;
#endif

        echoServPort = args->destPorts[i];

        /* Create a reliable, stream socket using TCP */
        if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
            throw new OutgoingSocketSetupError();
        }

        /* Construct the server address structure */
        memset(&echoServAddr, 0, sizeof(echoServAddr));     /* Zero out structure */
        echoServAddr.sin_family      = AF_INET;             /* Internet address family */
        echoServAddr.sin_addr.s_addr = inet_addr(args->destIPs->at(i));   /* Server IP address */
        echoServAddr.sin_port        = htons(echoServPort); /* Server port */

        /* Establish the connection to the echo server */
        while (connect(sock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr)) < 0){ 
        }   
        args->destSockets[i] = sock;
    }

#ifdef VERBOSE
    std::cout<<"setupConnThread done\n";
#endif

    delete args;
    return NULL;
};

void ShuffleOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	Operator::init(root, cfg);

	schema = nextOp->getOutSchema();

	//fieldno = cfg["field"];
	//--fieldno;
	//value = (long int) cfg["value"];

    producerStarted = false;

    //handle partition attribute and hashing stuff
    hashfn = TupleHasher::create(schema, cfg["hash"]);

    // handle the ip stuff 
    struct ifreq ifr;
    struct sockaddr_in saddr;
    int fd=socket(PF_INET,SOCK_STREAM,0);
    //netDEV variable for network device
    strcpy(ifr.ifr_name,(const char*)cfg["netDEV"]);
    ioctl(fd,SIOCGIFADDR,&ifr);
    saddr=*((struct sockaddr_in *)(&(ifr.ifr_addr)));

    myIP = new char[strlen(inet_ntoa(saddr.sin_addr))];
    strcpy(myIP, (const char*) cfg["myIP"]);

    //figure it out auto
    //strcpy(myIP, (inet_ntoa(saddr.sin_addr)));

#ifdef VERBOSE
    std::cout<<"myIP "<<myIP<<std::endl;
    std::cout<<(const char*)cfg["destIPs"]<<std::endl;
#endif
    csvDelim(destIPs, (const char*) cfg["destIPs"]);
#ifdef VERBOSE
    std::cout<<(const char*)cfg["incomingIPs"]<<std::endl;
#endif
    csvDelim(incomingIPs, (const char*) cfg["incomingIPs"]);

	incomingBasePort= (long int) cfg["incomingBasePort"];

    myDestOffset = -1;
    for (unsigned int i = 0 ; i < destIPs.size() ; i += 1)
	{
#ifdef VERBOSE
        std::cout<<destIPs[i]<<std::endl;
#endif
        if (strcmp(destIPs[i], myIP) == 0){
            myDestOffset = i;
        }

        noutput.push_back(new WillisBlock(buffsize, schema.getTupleSize()));

    }
    myIncomingOffset = -1;
    for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
	{
#ifdef VERBOSE
        std::cout<<incomingIPs[i]<<std::endl;
#endif
        if (strcmp(incomingIPs[i], myIP) == 0){
            myIncomingOffset = i;
        }
        //may need to store up to 2X data on input buffers 
        ninput.push_back(new WillisBlock(buffsize*2, schema.getTupleSize()));
    }
    nsendPage = new WillisBlock(buffsize, schema.getTupleSize());

#ifdef VERBOSE
    std::cout<<"destoffset "<<myDestOffset<<" inoffset "<<myIncomingOffset<<std::endl;
#endif

    lastIncomingSocket = -1;

    //setup sockets
    int flags;
    int destPorts[destIPs.size()];
    incomingSockets = new int[incomingIPs.size()];
    destSockets = new int[destIPs.size()];
    for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
	{
        incomingSockets[i] = CreateTCPServerSocket(incomingBasePort + myIncomingOffset + i);

        for (unsigned int j = 0 ; j < destIPs.size() ; j += 1)
		{
            if (strcmp(destIPs[j], incomingIPs[i]) == 0){
                destPorts[j] = incomingBasePort + myIncomingOffset + i;
            }
        }

        if ((flags = fcntl(incomingSockets[i], F_GETFL, 0)) < 0) 
        { 
            throw new IncomingSocketSetupError();
        } 


        if (fcntl(incomingSockets[i], F_SETFL, flags | O_NONBLOCK) < 0) 
        { 
            throw new IncomingSocketSetupError();
        } 
        if (incomingSockets[i] > lastIncomingSocket) lastIncomingSocket = incomingSockets[i];
    }

    //now need to kickstart a thread to open outputs

    pthread_t initConn;
    pthread_attr_t initConn_attr;
    pthread_attr_init(&initConn_attr);
    pthread_attr_setdetachstate(&initConn_attr, PTHREAD_CREATE_JOINABLE);
    struct initConnArgs *initConn_args;

    initConn_args = (struct initConnArgs *) malloc(sizeof (struct initConnArgs));
    initConn_args->numDestIPs = destIPs.size();
    initConn_args->destPorts = destPorts;
    initConn_args->destSockets = destSockets;
    initConn_args->destIPs = &destIPs;

    pthread_create(&initConn, &initConn_attr, setupConnThread, (void *) initConn_args);

    //done that


    num_accepted = 0;
    int tmpSocks[incomingIPs.size()];
    for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
	{
        tmpSocks[i] = -1;
    }
    //now finish the socket creation
    while(num_accepted < incomingIPs.size()){

        /* Zero socket descriptor vector and set for server sockets */
        /* This must be reset every time select() is called */
        FD_ZERO(&sockSet);
        for (unsigned int i = 0; i < incomingIPs.size(); i++)
		{
            FD_SET(incomingSockets[i], &sockSet);
        }

        /* Timeout specification */
        /* This must be reset every time select() is called */
        selTimeout.tv_sec = 10;       /* timeout (secs.) */

        if ((retval=select(lastIncomingSocket + 1, &sockSet, NULL, NULL, &selTimeout)) == 0){
        }
        else if (retval == -1) {
        } else {
            for (unsigned int i = 0; i < incomingIPs.size(); i++)
			{
                if ((tmpSocks[i] == -1) && FD_ISSET(incomingSockets[i], &sockSet)){
                    //accept because never seen before
                    struct sockaddr_in addr; 
                    unsigned int len;       

                    len = sizeof(addr);

                    /* Wait for a client to connect */
                    if ((tmpSocks[i] = ::accept(incomingSockets[i], (struct sockaddr *) &addr,&len)) < 0){
                    }

                    // BUG ALERT: accept returns a different sock #
                    incomingSockets[i] = tmpSocks[i];
                    if (incomingSockets[i] > lastIncomingSocket){
                        lastIncomingSocket = incomingSockets[i];  
                    }
                    num_accepted += 1;
                }
            }  
        }
    }
            

    //ColumnSpec cs = std::make_pair(CT_LONG, 0u);
	//comparator = Schema::createComparator(schema, fieldno, cs, Comparator::GreaterEqual);

	for (int i=0; i<MAX_THREADS; ++i) {
		output.push_back(NULL);
		state.push_back( State(NullPage, Ready, 0) );
	}
}

void ShuffleOp::threadInit(unsigned short threadid)
{
	output[threadid] = new WillisBlock(buffsize, schema.getTupleSize());
}

void ShuffleOp::threadClose(unsigned short threadid)
{
	if (output[threadid]) {
		delete output[threadid];
	}
	output[threadid] = NULL;
}

Operator::ResultCode ShuffleOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode;

	state[threadid] = State(&EmptyWillisBlock, Operator::Ready, 0);

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT ShuffleOp::getNext(unsigned short threadid)
{

    //can't start shuffle producer (calling getNext) safely unless shuffle consumer is called....?
    if (!producerStarted){

        pthread_t prod;//initConn;
        pthread_attr_t prod_attr;//initConn_attr;
        pthread_attr_init(&prod_attr);
        pthread_attr_setdetachstate(&prod_attr, PTHREAD_CREATE_JOINABLE);
        struct prodArgs *prod_args;
        prod_args = (struct prodArgs *) malloc(sizeof (struct prodArgs));
        prod_args->me = this;
#ifdef VERBOSE
        std::cout<<"threadid "<<threadid<<std::endl;
#endif
        prod_args->threadid = threadid;
#ifdef VERBOSE
        std::cout<<"threadid "<<prod_args->threadid<<std::endl;
#endif

        pthread_create(&prod, &prod_attr, prodThread, (void *) prod_args);


        producerStarted = true;
    }

    char rawbuf[buffsize];
    int amtRecv;


#ifdef VERBOSE
    std::cout<<num_accepted<<" active incoming sockets still open\n";
#endif

    while (num_accepted > 0){
        FD_ZERO(&sockSet);

        for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
		{
            if (incomingSockets[i]!= -1){
                FD_SET(incomingSockets[i], &sockSet);
            }
        }

        selTimeout.tv_sec = 5;

#ifdef VERBOSE
        std::cout<<"calling select\n";
#endif

        if ((retval=select(lastIncomingSocket + 1, &sockSet, NULL, NULL, &selTimeout)) == 0){ 
        }   
        else if (retval == -1) {
        }else{
            for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
			{
                if (incomingSockets[i]!= -1 && FD_ISSET(incomingSockets[i], &sockSet)){

                    amtRecv = recv(incomingSockets[i], rawbuf, buffsize, 0);
                    if (amtRecv < 0){
                        //throw something
                    }

#ifdef VERBOSE
                    std::cout<<"socket "<<i<<std::endl;
                    std::cout<<"eating "<<amtRecv<<std::endl;
                    std::cout<<"filled "<<ninput[i]->getFill()<<std::endl;
#endif
                    if (!ninput[i]->blockCopy(rawbuf, amtRecv)){
                        //throw something
#ifdef VERBOSE
                        std::cout<<"copy to input buffer no more space\n";
#endif
                    }

#ifdef VERBOSE
                    std::cout<<"received some data over net\n";
#endif

                    if (ninput[i]->getFill() >= buffsize)
					{
                        int overflow = ninput[i]->getFill() - buffsize;
#ifdef VERBOSE
                        std::cout<<ninput[i]->getFill()<<" filled "<<buffsize<<" page size "<<overflow<<" overflow\n";
#endif
                        output[threadid]->clear();
                        if (!output[threadid]->blockCopy(ninput[i], buffsize)){
                            //throw something
#ifdef VERBOSE
                            std::cout<<"copy to shuffle output no more space\n";
#endif
                        }
                        if (overflow > 0 && !ninput[i]->blockShift(ninput[i], overflow, buffsize)){
                            //throw something
#ifdef VERBOSE
                            std::cout<<"shift data left no more space\n";
#endif
                        }
                        if (overflow == 0){
                            ninput[i]->clear();
                        }

#ifdef VERBOSE
                        std::cout<<"filled "<<ninput[i]->getFill()<<std::endl;
                        std::cout<<"returning from shuffle\n";
#endif
                        return make_pair(Ready, output[threadid]);
                    }
                    if (amtRecv == 0){
                        num_accepted -= 1;
                        incomingSockets[i] = -1;
                    }
                }
            }
        }
    }


    //must clean up unfilled buffers, we know that the fill is less than buffsize
    output[threadid]->clear();
    unsigned int outputspace;
    for (unsigned int i = 0 ; i < incomingIPs.size() ; i += 1)
	{
#ifdef VERBOSE
        std::cout<<"how much left in bucket "<<i<<":  "<<ninput[i]->getFill()<<std::endl;
#endif

        if (ninput[i]->getFill() > 0){
            //how much can output handle
            outputspace = buffsize - output[threadid]->getFill();
            if (outputspace < ninput[i]->getFill()){
                output[threadid]->blockCopy(ninput[i], outputspace);
                ninput[i]->blockShift(ninput[i],ninput[i]->getFill() - outputspace, outputspace);
            }else{
                output[threadid]->blockCopy(ninput[i], ninput[i]->getFill());
                ninput[i]->clear();
            }

            if (!output[threadid]->canStoreTuple()){
                return make_pair(Ready, output[threadid]);
            }
        }
        /*
        while there is still data in the buff{
            return all the pages from this ip
        }
        */
    }

    return make_pair(Finished, output[threadid]);
}

bool ShuffleOp::WillisBlock::blockCopy(void * src, int len)
{
    if (!canStore((unsigned int) len)){
        return false;
    }
	memcpy(free, src, (unsigned int)len);
    free = reinterpret_cast<char*>(free) + len;
    return true;
}

bool ShuffleOp::WillisBlock::blockCopy(WillisBlock * src, int len)
{
    return (blockCopy(src->data, len));
}

bool ShuffleOp::WillisBlock::blockShift(WillisBlock * src, unsigned int len, int srcOffset)
{
    return (blockShift(src->data, len, srcOffset));
}

bool ShuffleOp::WillisBlock::blockShift(void * src, unsigned int len, int srcOffset)
{
    if (len > maxsize){
        return false;
    }
    if (len == 0){
        clear();
        return true;
    }
    std::cerr<<"shifting"<<std::endl;
    std::cerr<<"before shift "<<getFill()<<std::endl;

	memcpy(data, reinterpret_cast<char*>(src)+srcOffset , len);

    free = reinterpret_cast<char*>(data) + len;

    std::cerr<<"end shift "<<getFill()<<std::endl;
    
    return true;
}
