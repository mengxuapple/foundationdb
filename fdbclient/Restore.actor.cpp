/*
 * Restore.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "NativeAPI.h"
#include "SystemData.h"

//Backup agent header
#include "BackupAgent.h"
#include "RestoreInterface.h"
#include "FileBackupAgent.h"
#include "ManagementAPI.h"
#include "MutationList.h"

/*
#include "BackupContainer.h"
#include "DatabaseContext.h"
#include "ManagementAPI.h"
#include "Status.h"
#include "KeyBackedTypes.h"
*/


#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

//Backup agent header end

#include "flow/actorcompiler.h"  // This must be the last #include.

#define MX_DEBUG 1


//for convenience
typedef FileBackupAgent::ERestoreState ERestoreState;

template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val); // { return Tuple().append(val); }
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val); // { return (ERestoreState)val.getInt(0); }


static Future<Version> restoreMX(Database const &cx, RestoreRequest const &request);

//Future<Void> restoreAgentDB(Database cx, LocalityData const& locality)
ACTOR Future<Void>  restoreAgentDB(Database cx_input, LocalityData locality) {

	state Database cx = cx_input;
	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: restoreAgent starts. Try to be a leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentStartTryBeLeader").detail("Locality", locality.toString());
	state Transaction tr(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr.numErrors);
			Optional<Value> leader = wait(tr.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr.numErrors)
				.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr.commit());
			break;
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader. Locality:%s\n", locality.toString().c_str());
		TraceEvent("RestoreAgentNotLeader").detail("Locality", locality.toString());
		loop {
			try {
				tr.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr.commit());
				break;
			} catch( Error &e ) {
				wait( tr.onError(e) );
			}
		}

		loop {
			choose {
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("GotRequest", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData);
				}
			}
		}
	}

	//we are the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentIsLeader").detail("Locality", locality.toString());
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(decodeRestoreAgentValue(it.value));
				}
				break;
			}
			printf("MX: agents number:%d\n", agentValues.size());
			TraceEvent("RestoreAgentLeader").detail("AgentSize", agentValues.size());
			wait( delay(5.0) );
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	state int testData = 0;
	loop {
		wait(delay(1.0));
		printf("Sending Request: %d\n", testData);
		TraceEvent("RestoreAgentLeader").detail("SendingRequest", testData);
		std::vector<Future<TestReply>> replies;
		for(auto& it : agents) {
			replies.push_back( it.test.getReply(TestRequest(testData)) );
		}
		printf("Wait on all %d requests for testData %d\n", agents.size(), testData);

		std::vector<TestReply> reps = wait( getAll(replies )); //When agent or master dies, we may stuck here. This is why we have not handled fault
		printf("getTestReply number:%d\n", reps.size());
		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
		testData = reps[0].replyData;
	}

}

//MX: Hack: directly copy the function above with minor change
ACTOR Future<Void> restoreAgent(Reference<ClusterConnectionFile> ccf, LocalityData locality) {


	TraceEvent("RestoreAgent").detail("ClusterFile", ccf->getFilename());
	//Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1); //Cannot use filename to create cluster because the filename may not be found in the simulator when the function is invoked.
	Reference<Cluster> cluster = Cluster::createCluster(ccf, -1);
	state Database cx = wait(cluster->createDatabase(locality));
	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: restoreAgent starts. Try to be a leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentStartTryBeLeader").detail("Locality", locality.toString());
	state Transaction tr(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr.numErrors);
			Optional<Value> leader = wait(tr.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr.numErrors)
					.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr.commit());
			break;
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader. Locality:%s\n", locality.toString().c_str());
		TraceEvent("RestoreAgentNotLeader").detail("Locality", locality.toString());
		loop {
			try {
				tr.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr.commit());
				break;
			} catch( Error &e ) {
				wait( tr.onError(e) );
			}
		}

		loop {
			choose {
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("GotRequest", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData);
				}
			}
		}
	}

	//we are the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentIsLeader").detail("Locality", locality.toString());
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(decodeRestoreAgentValue(it.value));
				}
				break;
			}
			printf("MX: agents number:%d\n", agentValues.size());
			TraceEvent("RestoreAgentLeader").detail("AgentSize", agentValues.size());
			wait( delay(5.0) );
		} catch( Error &e ) {
			TraceEvent(SevError, "GetRestoreAgentsKeysErrorMX");
			wait( tr.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	state int testData = 0;
	loop {
		wait(delay(1.0));
		printf("Sending Request: %d\n", testData);
		TraceEvent("RestoreAgentLeader").detail("SendingRequest", testData);
		std::vector<Future<TestReply>> replies;
		for(auto& it : agents) {
			replies.push_back( it.test.getReply(TestRequest(testData)) );
		}
		printf("Wait on all %d requests for testData %d\n", agents.size(), testData);

		std::vector<TestReply> reps = wait( getAll(replies ));
		printf("getTestReply number:%d\n", reps.size());
		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
		testData = reps[0].replyData;
	}
}


//MX: This is where the restore code goes
//MX: Hack: directly copy the function above with minor change
//ACTOR Future<Void> restoreAgent_run(Database db) {
//ACTOR Future<Void> restoreAgent_run(Reference<ClusterConnectionFile> ccf, LocalityData locality) {
ACTOR Future<Void> restoreAgent_runDB(Database cx_input, LocalityData locality) {

	//TraceEvent("RestoreAgentRun");
//	TraceEvent("RestoreAgentRun").detail("ClusterFile", ccf->getFilename());
//	//Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1); //Cannot use filename to create cluster because the filename may not be found in the simulator when the function is invoked.
//	Reference<Cluster> cluster = Cluster::createCluster(ccf, -1);
//	state Database cx = wait(cluster->createDatabase(locality));
	state Database cx = cx_input;

	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: RestoreAgentRun starts. Try to be a leader.\n");
	TraceEvent("RestoreAgentStartTryBeLeader");
	state Transaction tr0(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr0.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr0.numErrors).detail("ReadLeaderKey", restoreLeaderKey.printable());
			Optional<Value> leader = wait(tr0.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr0.numErrors)
					.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr0.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr0.commit());
			break;
		} catch( Error &e ) {
			wait( tr0.onError(e) );
		}
	}

	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader.\n");
		TraceEvent("RestoreAgentNotLeader");

		loop {
			try {
				tr0.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr0.commit());
				break;
			} catch( Error &e ) {
				wait( tr0.onError(e) );
			}
		}

		return Void(); //TODO: This is to let agent return so that testers can check the correctness. Remove this line later

/*
		loop {
			choose {
				//Actual restore code
				when(RestoreRequest req = waitNext(interf.request.getFuture())) {
					printf("Got Restore Request: Index %d. RequestData:%d\n", req.testData, req.restoreRequests[req.testData]);
					TraceEvent("RestoreAgentNotLeader").detail("GotRestoreRequestID", req.testData)
						.detail("GotRestoreRequestValue", req.restoreRequests[req.testData]);
					//TODO: MX: actual restore
					std::vector<int> values(req.restoreRequests);
					values[req.testData]++;
					req.reply.send(RestoreReply(req.testData * -1, values));
					printf("Send Reply: %d, Value:%d\n", req.testData, values[req.testData]);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData).detail("Value", req.restoreRequests[req.testData]);
					if ( values[req.testData] > 10 ) { //MX: only calculate up to 10
						return Void();
					}
				}
				//Example code
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("GotRequest", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData);
				}
			}
		}
*/

	}

	//I am the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader.\n");
	TraceEvent("RestoreAgentIsLeader");
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	//state Transaction tr0(cx);
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr0.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(decodeRestoreAgentValue(it.value));
				}
				break;
			}
			printf("MX: agents number:%d\n", agentValues.size());
			TraceEvent("RestoreAgentLeader").detail("AgentSize", agentValues.size());
			wait( delay(5.0) );
		} catch( Error &e ) {
			TraceEvent("RestoreAgentLeaderErrorTr1").detail("ReadrestoreAgentsKeysFails", e.code()).detail("ErrorName", e.name());
			wait( tr0.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	//create initial point, The initial request value for agent i is i.
	state std::vector<int> restoreRequestsInt;
	for ( int i = 0; i < agents.size(); ++i ) {
		restoreRequestsInt.push_back(i);
	}

	// ----------------Restore code START
	state int restoreId = 0;
	state int checkNum = 0;
	loop {
		state vector<RestoreRequest> restoreRequests;
		loop {
			state Transaction tr2(cx);
			try {
				TraceEvent("CheckRestoreRequestTrigger");
				printf("CheckRestoreRequestTrigger:%d\n", checkNum);
				checkNum++;

				state Optional<Value> numRequests = wait(tr2.get(restoreRequestTriggerKey));
				if ( !numRequests.present() ) { // restore has not been triggered yet
					TraceEvent("CheckRestoreRequestTrigger").detail("SecondsOfWait", 5);
					wait( delay(5.0) );
					continue;
				}
				int num = decodeRestoreRequestTriggerValue(numRequests.get());
				TraceEvent("RestoreRequestKey").detail("NumRequests", num);
				Standalone<RangeResultRef> restoreRequestValues = wait(tr2.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!restoreRequestValues.more);

				if(restoreRequestValues.size()) {
					for ( auto &it : restoreRequestValues ) {
						restoreRequests.push_back(decodeRestoreRequestValue(it.value));
					}
				}
				break;
			} catch( Error &e ) {
				TraceEvent("RestoreAgentLeaderErrorTr2").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
				wait( tr2.onError(e) );
			}
		}
		// Perform the restore requests
		for ( auto &it : restoreRequests ) {
			TraceEvent("LeaderGotRestoreRequest").detail("RestoreRequestInfo", it.toString());
			Version ver = wait( restoreMX(cx, it) );
		}

		// Notify the finish of the restore by cleaning up the restore keys
		state Transaction tr3(cx);
		tr3.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr3.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			tr3.clear(restoreRequestTriggerKey);
			tr3.clear(restoreRequestKeys);
			tr3.set(restoreRequestDoneKey, restoreRequestDoneValue(restoreRequests.size()));
			TraceEvent("LeaderFinishRestoreRequest");
			printf("LeaderFinishRestoreRequest\n");
			wait(tr3.commit());
		}  catch( Error &e ) {
			TraceEvent("RestoreAgentLeaderErrorTr3").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
			wait( tr3.onError(e) );
		}

		printf("MXRestoreEndHere RestoreID:%d\n", restoreId);
		TraceEvent("MXRestoreEndHere").detail("RestoreID", restoreId++);
		wait( delay(5.0) );
		//NOTE: we have to break the loop so that the tester.actor can receive the return of this test workload.
		//Otherwise, this special workload never returns and tester will think the test workload is stuck and the tester will timesout
		break; //TODO: this break will be removed later since we need the restore agent to run all the time!

		//assert( 0 );
		//atomicRestoreMX();
	}

	return Void();

	// ----------------Restore code END


	// ---------------Increase counter example code
//	loop {
//		wait(delay(1.0));
//
//		printf("---Sending Requests\n");
//		TraceEvent("RestoreAgentLeader").detail("SendingRequests", "CheckBelow");
//		for (int i = 0; i < restoreRequestsInt.size(); ++i ) {
//			printf("RestoreRequests[%d]=%d\n", i, restoreRequestsInt[i]);
//			TraceEvent("RestoreRequests").detail("Index", i).detail("Value", restoreRequestsInt[i]);
//		}
//		std::vector<Future<RestoreReply>> replies;
//		for ( int i = 0; i < agents.size(); ++i) {
//			auto &it = agents[i];
//			replies.push_back( it.request.getReply(RestoreRequest(i, restoreRequestsInt)) );
//		}
//		printf("Wait on all %d requests\n", agents.size());
//
//		std::vector<RestoreReply> reps = wait( getAll(replies ));
//		printf("GetRestoreReply values\n", reps.size());
//		for ( int i = 0; i < reps.size(); ++i ) {
//			printf("RestoreReply[%d]=%d\n [checksum=%d]", i, reps[i].restoreReplies[i], reps[i].replyData);
//			//prepare to send the next request batch
//			restoreRequestsInt[i] = reps[i].restoreReplies[i];
//			if ( restoreRequestsInt[i] > 10 ) { //MX: only calculate up to 10
//				return Void();
//			}
//		}
//		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
//	}
}

/*

// This method will return the final status of the backup
ACTOR static Future<ERestoreState> restoreAgentWaitRestore(Database cx, Key tagName, bool verbose) {
	loop {
		try {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state KeyBackedTag tag = makeRestoreTag(tagName.toString());
			Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
			if(!current.present()) {
				if(verbose)
					printf("Tag: %s  State: %s\n", tagName.toString().c_str(), FileBackupAgent::restoreStateText(ERestoreState::UNITIALIZED).toString().c_str());
				return ERestoreState::UNITIALIZED;
			}

			state RestoreConfig restore(current.get().first);

			if(verbose) {
				state std::string details = wait(restore.getProgress(tr));
				printf("%s\n", details.c_str());
			}

			state ERestoreState status = wait(restore.stateEnum().getD(tr));
			state bool runnable = wait(restore.isRunnable(tr));

			// State won't change from here
			if (!runnable)
				break;

			// Wait for a change
			state Future<Void> watchFuture = tr->watch(restore.stateEnum().key);
			wait(tr->commit());
			if(verbose)
				wait(watchFuture || delay(1));
			else
				wait(watchFuture);
		}
		catch (Error &e) {
			wait(tr->onError(e));
		}
	}

	return status;
}

*/


//-------------------------CODE FOR RESTORE----------------------------

struct cmpForKVOps {
	bool operator()(const Version& a, const Version& b) const {
		return a < b;
	}
};

std::map<Version, Standalone<VectorRef<MutationRef>>> kvOps;
//std::map<Version, std::vector<MutationRef>> kvOps; //TODO: Must change to standAlone before run correctness test. otherwise, you will see the mutationref memory is corrupted
std::map<Standalone<StringRef>, Standalone<StringRef>> mutationMap; //key is the unique identifier for a batch of mutation logs at the same version
std::map<Standalone<StringRef>, uint32_t> mutationPartMap; //Record the most recent
// MXX: Important: Can not use std::vector because you won't have the arena and you will hold the reference to memory that will be freed.
// Use push_back_deep() to copy data to the standalone arena.
//Standalone<VectorRef<MutationRef>> mOps;
std::vector<MutationRef> mOps;

// Helper class for reading restore data from a buffer and throwing the right errors.
struct StringRefReaderMX {
	StringRefReaderMX(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

	// Return remainder of data as a StringRef
	StringRef remainder() {
		return StringRef(rptr, end - rptr);
	}

	// Return a pointer to len bytes at the current read position and advance read pos
	//Consume a little-Endian data. Since we only run on little-Endian machine, the data on storage is little Endian
	const uint8_t * consume(unsigned int len) {
		if(rptr == end && len != 0)
			throw end_of_stream();
		const uint8_t *p = rptr;
		rptr += len;
		if(rptr > end)
			throw failure_error;
		return p;
	}

	// Return a T from the current read position and advance read pos
	template<typename T> const T consume() {
		return *(const T *)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte order) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	const int32_t  consumeNetworkInt32()  { return (int32_t)bigEndian32((uint32_t)consume< int32_t>());}
	const uint32_t consumeNetworkUInt32() { return          bigEndian32(          consume<uint32_t>());}

	const int64_t  consumeNetworkInt64()  { return (int64_t)bigEndian64((uint32_t)consume< int64_t>());}
	const uint64_t consumeNetworkUInt64() { return          bigEndian64(          consume<uint64_t>());}

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	Error failure_error;
};

std::string getHexString(StringRef input) {
	std::stringstream ss;
	for (int i = 0; i<input.size(); i++) {
		if ( i % 4 == 0 )
			ss << " ";
		if ( i == 12 ) { //The end of 12bytes, which is the version size for value
			ss << "|";
		}
		if ( i == (12 + 12) ) { //The end of version + header
			ss << "@";
		}
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}

std::string getHexKey(StringRef input, int skip) {
	std::stringstream ss;
	for (int i = 0; i<skip; i++) {
		if ( i % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	ss << "||";

	//hashvalue
	ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[skip]; // [] operator moves the pointer in step of unit8
	ss << "|";

	// commitversion in 64bit
	int count = 0;
	for (int i = skip+1; i<input.size() && i < skip+1+8; i++) {
		if ( count++ % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	// part value
	count = 0;
	for (int i = skip+1+8; i<input.size(); i++) {
		if ( count++ % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}


void printMutationListRefHex(MutationListRef m, std::string prefix) {
	MutationListRef::Iterator iter = m.begin();
	for ( ;iter != m.end(); ++iter) {
		printf("%s mType:%04x param1:%s param2:%s param1_size:%d, param2_size:%d\n", prefix.c_str(), iter->type,
			   getHexString(iter->param1).c_str(), getHexString(iter->param2).c_str(), iter->param1.size(), iter->param2.size());
	}
}

//TODO: Print out the backup mutation log value. The backup log value (i.e., the value in the kv pair) has the following format
//version(12B)|mutationRef|MutationRef|....
//A mutationRef has the format: |type_4B|param1_size_4B|param2_size_4B|param1|param2.
//Note: The data is stored in little endian! You need to convert it to BigEndian so that you know how long the param1 and param2 is and how to format them!
void printBackupMutationRefValueHex(Standalone<StringRef> val_input, std::string prefix) {
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	printf("----------------------------------------------------------\n");
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		printf("%s[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n", prefix.c_str(), val_length_decode, val.size());
	} else {
		printf("%s[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", prefix.c_str(), val_length_decode, val.size());
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFF
			printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		}

		printf("%s---DedoceBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
				type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);

	}
	printf("----------------------------------------------------------\n");
}

void printBackupLogKeyHex(Standalone<StringRef> key_input, std::string prefix) {
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = key_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	printf("----------------------------------------------------------\n");
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		printf("%s[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n", prefix.c_str(), val_length_decode, val.size());
	} else {
		printf("%s[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", prefix.c_str(), val_length_decode, val.size());
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFF
			printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		}

		printf("%s---DedoceBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
			   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);

	}
	printf("----------------------------------------------------------\n");
}

void printKVOps() {
	std::string typeStr = "MSet";
	TraceEvent("PrintKVOPs").detail("MapSize", kvOps.size());
	printf("PrintKVOPs num_of_version:%d\n", kvOps.size());
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		TraceEvent("PrintKVOPs\t").detail("Version", it->first).detail("OpNum", it->second.size());
		printf("PrintKVOPs Version:%08lx num_of_ops:%d\n",  it->first, it->second.size());
		for ( auto m = it->second.begin(); m != it->second.end(); ++m ) {
			if (  m->type >= MutationRef::Type::SetValue && m->type <= MutationRef::Type::MAX_ATOMIC_OP )
				typeStr = typeString[m->type];
			else {
				printf("PrintKVOPs MutationType:%d is out of range\n", m->type);
			}

			printf("\tPrintKVOPs Version:%016lx MType:%s K:%s, V:%s K_size:%d V_size:%d\n", it->first, typeStr.c_str(),
				   getHexString(m->param1).c_str(), getHexString(m->param2).c_str(), m->param1.size(), m->param2.size());

			TraceEvent("PrintKVOPs\t\t").detail("Version", it->first)
				.detail("MType", m->type).detail("MTypeStr", typeStr)
				.detail("MKey", getHexString(m->param1))
				.detail("MValueSize", m->param2.size())
				.detail("MValue", getHexString(m->param2));
		}
	}
}

// Sanity check if KVOps is sorted
bool isKVOpsSorted() {
	bool ret = true;
	auto prev = kvOps.begin();
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		if ( prev->first > it->first ) {
			ret = false;
			break;
		}
		prev = it;
	}
	return ret;
}

bool allOpsAreKnown() {
	bool ret = true;
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		for ( auto m = it->second.begin(); m != it->second.end(); ++m ) {
			if ( m->type == MutationRef::SetValue || m->type == MutationRef::ClearRange  )
				continue;
			else {
				printf("[ERROR] Unknown mutation type:%d\n", m->type);
				ret = false;
			}
		}

	}

	return ret;
}

ACTOR Future<Void> applyKVOpsToDB(Database cx) {
	state bool isPrint = false;
	state std::string typeStr = "";

	TraceEvent("ApplyKVOPsToDB").detail("MapSize", kvOps.size());
	printf("ApplyKVOPsToDB num_of_version:%d\n", kvOps.size());
	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator it = kvOps.begin();
	state int count = 0;
	for ( ; it != kvOps.end(); ++it ) {

//		TraceEvent("ApplyKVOPsToDB\t").detail("Version", it->first).detail("OpNum", it->second.size());
		printf("ApplyKVOPsToDB Version:%08lx num_of_ops:%d\n",  it->first, it->second.size());

		state MutationRef m;
		state int index = 0;
		for ( ; index < it->second.size(); ++index ) {
			m = it->second[index];
			if (  m.type >= MutationRef::Type::SetValue && m.type <= MutationRef::Type::MAX_ATOMIC_OP )
				typeStr = typeString[m.type];
			else {
				printf("ApplyKVOPsToDB MutationType:%d is out of range\n", m.type);
			}

			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					if ( m.type == MutationRef::SetValue ) {
						tr->set(m.param1, m.param2);
					} else if ( m.type == MutationRef::ClearRange ) {
						KeyRangeRef mutationRange(m.param1, m.param2);
						tr->clear(mutationRange);
					} else {
						printf("[WARNING] mtype:%d (%s) unhandled\n", m.type, typeStr.c_str());
					}

					wait(tr->commit());
					break;
				} catch(Error &e) {
					printf("ApplyKVOPsToDB transaction error:%s. Type:%d, Param1:%s, Param2:%s\n", e.what(),
							m.type, getHexString(m.param1).c_str(), getHexString(m.param2).c_str());
					wait(tr->onError(e));
				}
			}

			if ( isPrint ) {
				printf("\tApplyKVOPsToDB Version:%016lx MType:%s K:%s, V:%s K_size:%d V_size:%d\n", it->first, typeStr.c_str(),
					   getHexString(m.param1).c_str(), getHexString(m.param2).c_str(), m.param1.size(), m.param2.size());

				TraceEvent("ApplyKVOPsToDB\t\t").detail("Version", it->first)
						.detail("MType", m.type).detail("MTypeStr", typeStr)
						.detail("MKey", getHexString(m.param1))
						.detail("MValueSize", m.param2.size())
						.detail("MValue", getHexString(m.param2));
			}
		}
	}

	return Void();
}

//version_input is the file version
void registerBackupMutation(Standalone<StringRef> val_input, Version file_version) {
	std::string prefix = "||\t";
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	if ( kvOps.find(file_version) == kvOps.end() ) {
		//kvOps.insert(std::make_pair(rangeFile.version, Standalone<VectorRef<MutationRef>>(VectorRef<MutationRef>())));
		kvOps.insert(std::make_pair(file_version, VectorRef<MutationRef>()));
	}

	printf("----------------------------------------------------------Register Backup Mutation into KVOPs version:%08lx\n", file_version);
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		printf("[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n",  val_length_decode, val.size());
	} else {
		printf("[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", val_length_decode, val.size());
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFF
			printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInkvOps[t32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		MutationRef m((MutationRef::Type) type, KeyRef(k, kLen), KeyRef(v, vLen)); //ASSUME: all operation in range file is set.
		kvOps[file_version].push_back_deep(kvOps[file_version].arena(), m);

//		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
//			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
//		}
//
		printf("%s---RegisterBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
			   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);

	}
//	printf("----------------------------------------------------------\n");
}

//key_input format: [logRangeMutation.first][hash_value_of_commit_version:1B][bigEndian64(commitVersion)][bigEndian32(part)]
void concatenateBackupMutation(Standalone<StringRef> val_input, Standalone<StringRef> key_input) {
	std::string prefix = "||\t";
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());
	StringRefReaderMX readerKey(key_input, restore_corrupted_data()); //read key_input!
	int logRangeMutationFirstLength = key_input.size() - 1 - 8 - 4;

	if ( logRangeMutationFirstLength < 0 ) {
		printf("[ERROR]!!! logRangeMutationFirstLength:%d < 0, key_input.size:%d\n", logRangeMutationFirstLength, key_input.size());
	}

	printf("[DEBUG] Process key_input:%s\n", getHexKey(key_input, logRangeMutationFirstLength).c_str());
	//PARSE key
	Standalone<StringRef> id_old = key_input.substr(0, key_input.size() - 4); //Used to sanity check the decoding of key is correct
	Standalone<StringRef> partStr = key_input.substr(key_input.size() - 4, 4); //part
	StringRefReaderMX readerPart(partStr, restore_corrupted_data());
	uint32_t part_direct = readerPart.consumeNetworkUInt32(); //Consume a bigEndian value
	printf("[DEBUG] Process prefix:%s and partStr:%s part_direct:%08x fromm key_input:%s, size:%d\n",
			getHexKey(id_old, logRangeMutationFirstLength).c_str(),
			getHexString(partStr).c_str(),
		    part_direct,
			getHexKey(key_input, logRangeMutationFirstLength).c_str(),
		    key_input.size());

	StringRef longRangeMutationFirst;

	if ( logRangeMutationFirstLength > 0 ) {
		printf("readerKey consumes %dB\n", logRangeMutationFirstLength);
		longRangeMutationFirst = StringRef(readerKey.consume(logRangeMutationFirstLength), logRangeMutationFirstLength);
	}

	uint8_t hashValue = readerKey.consume<uint8_t>();
	uint64_t commitVersion = readerKey.consumeNetworkUInt64(); // Consume big Endian value encoded in log file, commitVersion is in littleEndian
	uint64_t commitVersionBE = bigEndian64(commitVersion);
	uint32_t part = readerKey.consumeNetworkUInt32(); //Consume big Endian value encoded in log file
	uint32_t partBE = bigEndian32(part);
	Standalone<StringRef> id2 = longRangeMutationFirst.withSuffix(StringRef(&hashValue,1)).withSuffix(StringRef((uint8_t*) &commitVersion, 8));

	//Use commitVersion as id
	Standalone<StringRef> id = StringRef((uint8_t*) &commitVersion, 8);

	printf("[DEBUG] key_input_size:%d longRangeMutationFirst:%s hashValue:%02x commitVersion:%016lx (BigEndian:%016lx) part:%08x (BigEndian:%08x), part_direct:%08x mutationMap.size:%d\n",
			key_input.size(), longRangeMutationFirst.printable().c_str(), hashValue,
			commitVersion, commitVersionBE,
			part, partBE,
			part_direct, mutationMap.size());

	if ( mutationMap.find(id) == mutationMap.end() ) {
		mutationMap.insert(std::make_pair(id, val_input));
		if ( part_direct != 0 ) {
			printf("[ERROR]!!! part:%d != 0 for key_input:%s\n", part, getHexString(key_input).c_str());
		}
		mutationPartMap.insert(std::make_pair(id, part));
	} else { // concatenate the val string
		mutationMap[id] = mutationMap[id].contents().withSuffix(val_input.contents()); //Assign the new Areana to the map's value
		if ( part_direct != (mutationPartMap[id] + 1) ) {
			printf("[ERROR]!!! current part id:%d new part_direct:%d is not the next integer of key_input:%s\n", mutationPartMap[id], part_direct, getHexString(key_input).c_str());
		}
		if ( part_direct != part ) {
			printf("part_direct:%08x != part:%08x\n", part_direct, part);
		}
		mutationPartMap[id] = part;
	}
}

void registerBackupMutationForAll(Version empty) {
	std::string prefix = "||\t";
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;

	for ( auto& m: mutationMap ) {
		StringRef k = m.first.contents();
		StringRefReaderMX readerVersion(k, restore_corrupted_data());
		uint64_t commitVerison = readerVersion.consume<uint64_t>(); // Consume little Endian data


		StringRef val = m.second.contents();
		StringRefReaderMX reader(val, restore_corrupted_data());

		int count_size = 0;
		// Get the include version in the batch commit, which is not the commitVersion.
		// commitVersion is in the key
		uint64_t includeVersion = reader.consume<uint64_t>();
		count_size += 8;
		uint32_t val_length_decode = reader.consume<uint32_t>(); //Parse little endian value, confirmed it is correct!
		count_size += 4;

		if ( kvOps.find(commitVerison) == kvOps.end() ) {
			kvOps.insert(std::make_pair(commitVerison, VectorRef<MutationRef>()));
		}

		printf("----------------------------------------------------------Register Backup Mutation into KVOPs version:%08lx\n", commitVerison);
		printf("To decode value:%s\n", getHexString(val).c_str());
		if ( val_length_decode != (val.size() - 12) ) {
			//IF we see val.size() == 10000, It means val should be concatenated! The concatenation may fail to copy the data
			printf("[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n",  val_length_decode, val.size());
		} else {
			printf("[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", val_length_decode, val.size());
		}

		// Get the mutation header
		while (1) {
			// stop when reach the end of the string
			if(reader.eof() ) { //|| *reader.rptr == 0xFF
				printf("Finish decode the value\n");
				break;
			}


			uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
			uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInkvOps[t32();
			uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
			const uint8_t *k = reader.consume(kLen);
			const uint8_t *v = reader.consume(vLen);
			count_size += 4 * 3 + kLen + vLen;

			MutationRef m((MutationRef::Type) type, KeyRef(k, kLen), KeyRef(v, vLen)); //ASSUME: all operation in range file is set.
			kvOps[commitVerison].push_back_deep(kvOps[commitVerison].arena(), m);

//		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
//			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
//		}
//
			printf("%s---RegisterBackupMutation: Version:%016lx Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
				   commitVerison, type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);

		}
//	printf("----------------------------------------------------------\n");
	}



}


void filterAndSortMutationOps() {
	std::string typeStr = "MSet";
	mOps.clear();
	TraceEvent("FilterAndSortMutationOps").detail("MapSize", kvOps.size());
	printf("FilterAndSortMutationOps, mapsSize:%d\n", kvOps.size());
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		TraceEvent("PrintKVOPs\t").detail("Version", it->first).detail("OpNum", it->second.size());
		printf("PrintKVOPs, k:%08lx v_size:%d\n", it->first, it->second.size());
		for ( auto m = it->second.begin(); m != it->second.end(); ++m ) {
			if ( m->type == MutationRef::Type::SetValue) {
				continue;
			} else if ( m->type != MutationRef::Type::NoOp ) { //Mutation log op is marked as NoOp. The actual op is in the mutationRef value
				printf("Something went wrong with the operation decoded from range and log files. Continue though\n");
				continue;
			}

			typeStr = "MNoOp";

			TraceEvent("DecodeKVOps").detail("Version", it->first)
					.detail("MType", m->type).detail("MTypeStr", typeStr)
					.detail("MKey", getHexString(m->param1))
					.detail("MValueSize", m->param2.size())
					.detail("MValue", getHexString(m->param2));

			StringRef param1 = m->param1;
			StringRef param2 = m->param2;
			//decode the param2 which is the serialized mutationListRef
			printf("\tMutationLog param1:%s\n", param1);
			printBackupMutationRefValueHex(param2, " |\t");

			//decode the value
			// Possible helper functions are
			// std::pair<uint64_t, uint32_t> decodeBKMutationLogKey(Key key)
			// Standalone<VectorRef<MutationRef>> decodeBackupLogValue(StringRef value)
//			std::pair<uint64_t, uint32_t> versionPart = decodeBKMutationLogKey(k);
//			TraceEvent("DecodeMutationKV").detail("Version", versionPart.first).detail("Part", versionPart.second);
//
//			std::pair<uint64_t, uint32_t> versionPart2 = decodeBKMutationLogKey(v);
//			TraceEvent("DecodeMutationKV2").detail("Version", versionPart2.first).detail("Part", versionPart2.second);


			/*
			Standalone<VectorRef<MutationRef>> mutations =  decodeBackupLogValue(v);
			TraceEvent("DecodeMutationKV").detail("Version", versionPart.first).detail("Part", versionPart.second)
				.detail("Mutation", mutations.contents().size());
			 */
		}
	}
}


//--- Apply backup file to DB system space
// NOTE: 10/29: This function is stalled. We may need to write our own function to parse the range file
ACTOR static Future<Void> _executeApplyRangeFileToDB(Database cx, RestoreConfig restore_input, Reference<Task> task,
													 RestoreFile rangeFile_input, int64_t readOffset_input, int64_t readLen_input,
													 Reference<IBackupContainer> bc, KeyRange restoreRange, Key addPrefix, Key removePrefix
													 ) {
	TraceEvent("ExecuteApplyRangeFileToDB_MX").detail("RestoreRange", restoreRange.contents().toString()).detail("AddPrefix", addPrefix.printable()).detail("RemovePrefix", removePrefix.printable());


	//state RestoreConfig restore(task);
	state RestoreConfig restore = restore_input;
	state RestoreFile rangeFile = rangeFile_input;
	state int64_t readOffset = readOffset_input;
	state int64_t readLen = readLen_input;


//	state RestoreFile rangeFile = Params.inputFile().get(task);
//	state int64_t readOffset = Params.readOffset().get(task);
//	state int64_t readLen = Params.readLen().get(task);

	TraceEvent("FileRestoreRangeStart_MX")
			.suppressFor(60)
			.detail("RestoreUID", restore.getUid())
			.detail("FileName", rangeFile.fileName)
			.detail("FileVersion", rangeFile.version)
			.detail("FileSize", rangeFile.fileSize)
			.detail("ReadOffset", readOffset)
			.detail("ReadLen", readLen)
			.detail("TaskInstance", (uint64_t)this);
	//MXX: the set of key value version is rangeFile.version. the key-value set in the same range file has the same version

//	state Reference<ReadYourWritesTransaction> tr( new ReadYourWritesTransaction(cx) );
//	state Future<Reference<IBackupContainer>> bc;
//	state Future<KeyRange> restoreRange;
//	state Future<Key> addPrefix;
//	state Future<Key> removePrefix;

//	loop {
//		try {
//			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
//			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

//
//			TraceEvent("Getbc");
//			bc = restore.sourceContainer().getOrThrow(tr);
//			TraceEvent("GetRestoreRange");
//			restoreRange = restore.restoreRange().getD(tr);
//			TraceEvent("GetAddPrefix");
//			addPrefix = restore.addPrefix().getD(tr);
//			TraceEvent("GetRemovePrefix");
//			removePrefix = restore.removePrefix().getD(tr);
//			TraceEvent("WaitOnSuccess");
//
//			//wait(taskBucket->keepRunning(tr, task));
//
//			//wait(success(bc) && success(restoreRange) && success(addPrefix) && success(removePrefix) && checkTaskVersion(tr->getDatabase(), task, name, version));
//			wait(success(bc));
//			// MX: Maybe because you didn't call addTask() to set the task, the task is not correctly constructed and that's why it stalls at getting bc.
//			TraceEvent("WaitOnSuccessBC");
//			wait(success(restoreRange));
//			TraceEvent("WaitOnSuccessRestoreRange");
//			wait(success(addPrefix));
//			TraceEvent("WaitOnSuccessAddPrefix");
//			wait(success(removePrefix));
//			TraceEvent("WaitOnSuccessRemovePrefix");
			//wait(success(bc) && success(restoreRange) && success(addPrefix) && success(removePrefix));
//			break;
//
//		} catch(Error &e) {
//			wait(tr->onError(e));
//		}
//	}

	TraceEvent("ReadFileStart").detail("Filename", rangeFile.fileName);
	state Reference<IAsyncFile> inFile = wait(bc->readFile(rangeFile.fileName));
	TraceEvent("ReadFileFinish").detail("Filename", rangeFile.fileName).detail("FileRefValid", inFile.isValid());

//	state int64_t beginBlock = 0;
//	state int64_t j = beginBlock *rangeFile.blockSize;
//	// For each block of the file
//	for(; j < rangeFile.fileSize; j += rangeFile.blockSize) {
//		readOffset = j;
//		readLen = std::min<int64_t>(rangeFile.blockSize, rangeFile.fileSize - j);
//		state Standalone<VectorRef<KeyValueRef>> blockData = wait(fileBackup::decodeRangeFileBlock(inFile, readOffset, readLen));
//		//TODO: Decode the Key-Value from data and assign it to the mutationMap
//		// Increment beginBlock for the file
//		++beginBlock;
//
//		TraceEvent("ReadLogFileFinish").detail("RangeFileName", rangeFile.fileName).detail("DecodedDataSize", blockData.contents().size());
//		TraceEvent("ApplyRangeFileToDB_MX").detail("BlockDataVectorSize", blockData.contents().size())
//				.detail("RangeFirstKey", blockData.front().key.printable()).detail("RangeLastKey", blockData.back().key.printable());
//	}


	state Standalone<VectorRef<KeyValueRef>> blockData = wait(fileBackup::decodeRangeFileBlock(inFile, readOffset, readLen));
	TraceEvent("ApplyRangeFileToDB_MX").detail("BlockDataVectorSize", blockData.contents().size())
			.detail("RangeFirstKey", blockData.front().key.printable()).detail("RangeLastKey", blockData.back().key.printable());


	// First and last key are the range for this file
	state KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

	// If fileRange doesn't intersect restore range then we're done.
	if(!fileRange.intersects(restoreRange)) {
		TraceEvent("ApplyRangeFileToDB_MX").detail("NoIntersectRestoreRange", "FinishAndReturn");
		return Void();
	}

	// We know the file range intersects the restore range but there could still be keys outside the restore range.
	// Find the subvector of kv pairs that intersect the restore range.  Note that the first and last keys are just the range endpoints for this file
	int rangeStart = 1;
	int rangeEnd = blockData.size() - 1;
	// Slide start forward, stop if something in range is found
	while(rangeStart < rangeEnd && !restoreRange.contains(blockData[rangeStart].key))
		++rangeStart;
	// Side end backward, stop if something in range is found
	while(rangeEnd > rangeStart && !restoreRange.contains(blockData[rangeEnd - 1].key))
		--rangeEnd;

	//MX: This is where the range file is splitted into smaller pieces
	state VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);

	// Shrink file range to be entirely within restoreRange and translate it to the new prefix
	// First, use the untranslated file range to create the shrunk original file range which must be used in the kv range version map for applying mutations
	state KeyRange originalFileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.begin), std::min(fileRange.end,   restoreRange.end));
//	Params.originalFileRange().set(task, originalFileRange);

	// Now shrink and translate fileRange
	Key fileEnd = std::min(fileRange.end,   restoreRange.end);
	if(fileEnd == (removePrefix == StringRef() ? normalKeys.end : strinc(removePrefix)) ) {
		fileEnd = addPrefix == StringRef() ? normalKeys.end : strinc(addPrefix);
	} else {
		fileEnd = fileEnd.removePrefix(removePrefix).withPrefix(addPrefix);
	}
	fileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.begin).removePrefix(removePrefix).withPrefix(addPrefix),fileEnd);

	state int start = 0;
	state int end = data.size();
	state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

//	tr->reset();
	//MX: This is where the key-value pair in range file is applied into DB
	TraceEvent("ApplyRangeFileToDB_MX").detail("Progress", "StartApplyKVToDB").detail("DataSize", data.size()).detail("DataSizeLimit", dataSizeLimit);
	loop {
//		try {
//			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
//			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state int i = start;
			state int txBytes = 0;
			state int iend = start;

			// find iend that results in the desired transaction size
			for(; iend < end && txBytes < dataSizeLimit; ++iend) {
				txBytes += data[iend].key.expectedSize();
				txBytes += data[iend].value.expectedSize();
			}

			// Clear the range we are about to set.
			// If start == 0 then use fileBegin for the start of the range, else data[start]
			// If iend == end then use fileEnd for the end of the range, else data[iend]
			state KeyRange trRange = KeyRangeRef((start == 0 ) ? fileRange.begin : data[start].key.removePrefix(removePrefix).withPrefix(addPrefix)
					, (iend == end) ? fileRange.end   : data[iend ].key.removePrefix(removePrefix).withPrefix(addPrefix));

//			tr->clear(trRange);

			for(; i < iend; ++i) {
//				tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
//				tr->set(data[i].key.removePrefix(removePrefix).withPrefix(addPrefix), data[i].value);
				//MXX: print out the key value version, and operations.
//				printf("RangeFile [key:%s, value:%s, version:%ld, op:set]\n", data[i].key.printable().c_str(), data[i].value.printable().c_str(), rangeFile.version);
				TraceEvent("PrintRangeFile_MX").detail("Key", data[i].key.printable()).detail("Value", data[i].value.printable())
					.detail("Version", rangeFile.version).detail("Op", "set");
				MutationRef m(MutationRef::Type::SetValue, data[i].key, data[i].value); //ASSUME: all operation in range file is set.
				if ( kvOps.find(rangeFile.version) == kvOps.end() ) {
					//kvOps.insert(std::make_pair(rangeFile.version, Standalone<VectorRef<MutationRef>>(VectorRef<MutationRef>())));
					kvOps.insert(std::make_pair(rangeFile.version, VectorRef<MutationRef>()));
				} else {
					//kvOps[rangeFile.version].contents().push_back_deep(m);
					kvOps[rangeFile.version].push_back_deep(kvOps[rangeFile.version].arena(), m);
				}

			}

			// Add to bytes written count
//			restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);
//
//			state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

//			wait(taskBucket->keepRunning(tr, task));

//			wait( checkLock );

//			wait(tr->commit());

			TraceEvent("FileRestoreCommittedRange_MX")
					.suppressFor(60)
					.detail("RestoreUID", restore.getUid())
					.detail("FileName", rangeFile.fileName)
					.detail("FileVersion", rangeFile.version)
					.detail("FileSize", rangeFile.fileSize)
					.detail("ReadOffset", readOffset)
					.detail("ReadLen", readLen)
//					.detail("CommitVersion", tr->getCommittedVersion())
					.detail("BeginRange", printable(trRange.begin))
					.detail("EndRange", printable(trRange.end))
					.detail("StartIndex", start)
					.detail("EndIndex", i)
					.detail("DataSize", data.size())
					.detail("Bytes", txBytes)
					.detail("OriginalFileRange", printable(originalFileRange));
//					.detail("TaskInstance", (uint64_t)this);


			TraceEvent("ApplyRangeFileToDBEnd_MX").detail("KVOpsMapSizeMX", kvOps.size()).detail("MutationSize", kvOps[rangeFile.version].size());

			// Commit succeeded, so advance starting point
			start = i;

			if(start == end) {
				TraceEvent("ApplyRangeFileToDB_MX").detail("Progress", "DoneApplyKVToDB");
				return Void();
			}
//			tr->reset();
//		} catch(Error &e) {
//			if(e.code() == error_code_transaction_too_large)
//				dataSizeLimit /= 2;
//			else
//				wait(tr->onError(e));
//		}
	}
}

ACTOR static Future<Void> _executeApplyMutationLogFileToDB(Database cx, Reference<Task> task,
														   RestoreFile logFile_input, int64_t readOffset_input, int64_t readLen_input,
														   Reference<IBackupContainer> bc, KeyRange restoreRange, Key addPrefix, Key removePrefix
														   ) {
	state RestoreConfig restore(task);

	state RestoreFile logFile = logFile_input;
	state int64_t readOffset = readOffset_input;
	state int64_t readLen = readLen_input;

	TraceEvent("FileRestoreLogStart_MX")
			.suppressFor(60)
			.detail("RestoreUID", restore.getUid())
			.detail("FileName", logFile.fileName)
			.detail("FileBeginVersion", logFile.version)
			.detail("FileEndVersion", logFile.endVersion)
			.detail("FileSize", logFile.fileSize)
			.detail("ReadOffset", readOffset)
			.detail("ReadLen", readLen)
			.detail("TaskInstance", (uint64_t)this);

//	state Reference<ReadYourWritesTransaction> tr( new ReadYourWritesTransaction(cx) );
//	state Reference<IBackupContainer> bc;

//	loop {
//		try {
//			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
//			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
//
////			Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
////			bc = _bc;
//
////			wait(checkTaskVersion(tr->getDatabase(), task, name, version));
////			wait(taskBucket->keepRunning(tr, task));
//
////			break;
//		} catch(Error &e) {
//			wait(tr->onError(e));
//		}
////	}

	state Key mutationLogPrefix = restore.mutationLogPrefix();
	TraceEvent("ReadLogFileStart").detail("LogFileName", logFile.fileName);
	state Reference<IAsyncFile> inFile = wait(bc->readFile(logFile.fileName));
	TraceEvent("ReadLogFileFinish").detail("LogFileName", logFile.fileName).detail("FileInfo", logFile.toString());

//	state RestoreFile f = logFile_input;
/*
	state int64_t beginBlock = 0;
	state int64_t j = beginBlock *logFile.blockSize;
	// For each block of the file
	for(; j < logFile.fileSize; j += logFile.blockSize) {
		readOffset = j;
		readLen = std::min<int64_t>(logFile.blockSize, logFile.fileSize - j);
		state Standalone<VectorRef<KeyValueRef>> data = wait(fileBackup::decodeLogFileBlock(inFile, readOffset, readLen));
		//TODO: Decode the Key-Value from data and assign it to the mutationMap
		// Increment beginBlock for the file
		++beginBlock;

		TraceEvent("ReadLogFileFinish").detail("LogFileName", logFile.fileName).detail("DecodedDataSize", data.contents().size());
	}
*/


	state Standalone<VectorRef<KeyValueRef>> data = wait(fileBackup::decodeLogFileBlock(inFile, readOffset, readLen));
	//state Standalone<VectorRef<MutationRef>> data = wait(fileBackup::decodeLogFileBlock_MX(inFile, readOffset, readLen)); //Decode log file
	TraceEvent("ReadLogFileFinish").detail("LogFileName", logFile.fileName).detail("DecodedDataSize", data.contents().size());

	state int start = 0;
	state int end = data.size();
	state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;



//	tr->reset();
	loop {
//		try {
			printf("Process start:%d where end=%d\n", start, end);
			if(start == end)
				return Void();

//			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
//			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state int i = start;
			state int txBytes = 0;
			for(; i < end && txBytes < dataSizeLimit; ++i) {
				Key k = data[i].key.withPrefix(mutationLogPrefix);
				ValueRef v = data[i].value;
//				tr->set(k, v);
				txBytes += k.expectedSize();
				txBytes += v.expectedSize();
				//MXX: print out the key value version, and operations.
				//printf("LogFile [key:%s, value:%s, version:%ld, op:NoOp]\n", k.printable().c_str(), v.printable().c_str(), logFile.version);
//				printf("LogFile [KEY:%s, VALUE:%s, VERSION:%ld, op:NoOp]\n", getHexString(k).c_str(), getHexString(v).c_str(), logFile.version);
//				printBackupMutationRefValueHex(v, " |\t");
/*
				TraceEvent("PrintMutationLogFile_MX").detail("Key",  getHexString(k)).detail("Value", getHexString(v))
						.detail("Version", logFile.version).detail("Op", "NoOps");

				printf("||Register backup mutation:file:%s, data:%d\n", logFile.fileName.c_str(), i);
				registerBackupMutation(data[i].value, logFile.version);
*/
				printf("[DEBUG]||Concatenate backup mutation:fileInfo:%s, data:%d\n", logFile.toString().c_str(), i);
				concatenateBackupMutation(data[i].value, data[i].key);
//				//TODO: Decode the value to get the mutation type. Use NoOp to distinguish from range kv for now.
//				MutationRef m(MutationRef::Type::NoOp, data[i].key, data[i].value); //ASSUME: all operation in log file is NoOp.
//				if ( kvOps.find(logFile.version) == kvOps.end() ) {
//					kvOps.insert(std::make_pair(logFile.version, std::vector<MutationRef>()));
//				} else {
//					kvOps[logFile.version].push_back(m);
//				}
			}

//			state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

//			wait(taskBucket->keepRunning(tr, task));
//			wait( checkLock );

			// Add to bytes written count
//			restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

//			wait(tr->commit());

			TraceEvent("FileRestoreCommittedLog")
					.suppressFor(60)
					.detail("RestoreUID", restore.getUid())
					.detail("FileName", logFile.fileName)
					.detail("FileBeginVersion", logFile.version)
					.detail("FileEndVersion", logFile.endVersion)
					.detail("FileSize", logFile.fileSize)
					.detail("ReadOffset", readOffset)
					.detail("ReadLen", readLen)
//					.detail("CommitVersion", tr->getCommittedVersion())
					.detail("StartIndex", start)
					.detail("EndIndex", i)
					.detail("DataSize", data.size())
					.detail("Bytes", txBytes);
//					.detail("TaskInstance", (uint64_t)this);

			TraceEvent("ApplyLogFileToDBEnd_MX").detail("KVOpsMapSizeMX", kvOps.size()).detail("MutationSize", kvOps[logFile.version].size());

			// Commit succeeded, so advance starting point
			start = i;
//			tr->reset();
//		} catch(Error &e) {
//			if(e.code() == error_code_transaction_too_large)
//				dataSizeLimit /= 2;
//			else
//				wait(tr->onError(e));
//		}
	}

}


ACTOR static Future<Void> _finishApplyRangeFileToDB(Reference<ReadYourWritesTransaction> tr,  Reference<Task> task) {
	state RestoreConfig restore(task);
	restore.fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

	// MXQ: Why should we do this?
	// Update the KV range map if originalFileRange is set
//	Future<Void> updateMap = Void();
//	if(Params.originalFileRange().exists(task)) {
//		Value versionEncoded = BinaryWriter::toValue(Params.inputFile().get(task).version, Unversioned());
//		updateMap = krmSetRange(tr, restore.applyMutationsMapPrefix(), Params.originalFileRange().get(task), versionEncoded);
//	}

//	state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
//	wait(taskFuture->set(tr, taskBucket) &&
//		 taskBucket->finish(tr, task) && updateMap);

	return Void();
}

ACTOR static Future<Void> _finishApplyMutationLogFileToDB(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
	RestoreConfig(task).fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

//	state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

	// TODO:  Check to see if there is a leak in the FutureBucket since an invalid task (validation key fails) will never set its taskFuture.
//	wait(taskFuture->set(tr, taskBucket) &&
//		 taskBucket->finish(tr, task));

	return Void();
}

//--- Prepare and get the backup file content
ACTOR static Future<Void> _executeMX(Database cx,  Reference<Task> task, UID uid, RestoreRequest request) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
//	state RestoreConfig restore(task);
	state RestoreConfig restore(uid);
	state Version restoreVersion;
	state Reference<IBackupContainer> bc;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state KeyRange restoreRange = request.range;

	TraceEvent("ExecuteMX");

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			//wait(checkTaskVersion(tr->getDatabase(), task, name, version));
			Version _restoreVersion = wait(restore.restoreVersion().getOrThrow(tr)); //Failed
			restoreVersion = _restoreVersion;
			TraceEvent("ExecuteMX").detail("RestoreVersion", restoreVersion);
			//wait(taskBucket->keepRunning(tr, task));

			ERestoreState oldState = wait(restore.stateEnum().getD(tr));
			TraceEvent("ExecuteMX").detail("OldState", oldState);
			if(oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
				wait(restore.logError(cx, restore_error(), format("StartFullRestore: Encountered unexpected state(%d)", oldState), this));
				TraceEvent("StartFullRestoreMX").detail("Error", "Encounter unexpected state");
				return Void();
			}
			restore.stateEnum().set(tr, ERestoreState::STARTING);
			TraceEvent("ExecuteMX").detail("StateEnum", "Done");
			restore.fileSet().clear(tr);
			restore.fileBlockCount().clear(tr);
			restore.fileCount().clear(tr);
			TraceEvent("ExecuteMX").detail("Clear", "Done");
			Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
			TraceEvent("ExecuteMX").detail("BackupContainer", "Done");
			bc = _bc;

			wait(tr->commit());
			break;
		} catch(Error &e) {
			TraceEvent("ExecuteMXErrorTr").detail("ErrorName", e.name());
			wait(tr->onError(e));
			TraceEvent("ExecuteMXErrorTrDone");
		}
	}

	TraceEvent("ExecuteMX").detail("GetRestoreSet", restoreVersion);

	//MX: Get restore file set from BackupContainer
	Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(restoreVersion));

	TraceEvent("ExecuteMX").detail("Restorable", restorable.present());

	if(!restorable.present())
		throw restore_missing_data();

	// First version for which log data should be applied
//	Params.firstVersion().set(task, restorable.get().snapshot.beginVersion);

	// Convert the two lists in restorable (logs and ranges) to a single list of RestoreFiles.
	// Order does not matter, they will be put in order when written to the restoreFileMap below.
	state std::vector<RestoreConfig::RestoreFile> files;

	for(const RangeFile &f : restorable.get().ranges) {
		TraceEvent("FoundRangeFileMX").detail("FileInfo", f.toString());
		files.push_back({f.version, f.fileName, true, f.blockSize, f.fileSize});
	}
	for(const LogFile &f : restorable.get().logs) {
		TraceEvent("FoundLogFileMX").detail("FileInfo", f.toString());
		files.push_back({f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion});
	}

	state std::vector<RestoreConfig::RestoreFile>::iterator start = files.begin();
	state std::vector<RestoreConfig::RestoreFile>::iterator end = files.end();

	tr->reset();
	while(start != end) {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			//wait(taskBucket->keepRunning(tr, task));

			state std::vector<RestoreConfig::RestoreFile>::iterator i = start;

			state int txBytes = 0;
			state int nFileBlocks = 0;
			state int nFiles = 0;
			auto fileSet = restore.fileSet();
			for(; i != end && txBytes < 1e6; ++i) {
				txBytes += fileSet.insert(tr, *i);
				nFileBlocks += (i->fileSize + i->blockSize - 1) / i->blockSize;
				++nFiles;
			}

			// Increment counts
			restore.fileCount().atomicOp(tr, nFiles, MutationRef::Type::AddValue);
			restore.fileBlockCount().atomicOp(tr, nFileBlocks, MutationRef::Type::AddValue);

			wait(tr->commit());

			TraceEvent("FileRestoreLoadedFilesMX")
					.detail("RestoreUID", restore.getUid())
					.detail("FileCount", nFiles)
					.detail("FileBlockCount", nFileBlocks)
					.detail("TransactionBytes", txBytes)
					.detail("TaskInstance", (uint64_t)this);

			start = i;
			tr->reset();
		} catch(Error &e) {
			wait(tr->onError(e));
		}
	}

	//Apply range and log files to DB
	TraceEvent("ApplyBackupFileToDB").detail("FileSize", files.size());
	printf("ApplyBackupFileToDB, FileSize:%d\n", files.size());
	state int64_t beginBlock = 0;
	state int64_t j = 0;
	state int64_t readLen = 0;
	state int64_t readOffset = 0;
	state RestoreConfig::RestoreFile f;
	state int fi = 0;
	//Get the mutation log into the kvOps first
	printf("ApplyMutationLogs\n");
	state std::vector<Future<Void>> futures;
	for ( fi = 0; fi < files.size(); ++fi ) {
		f = files[fi];
		if ( !f.isRange ) {
			TraceEvent("ApplyLogFileToDB_MX").detail("FileInfo", f.toString());
			printf("ApplyMutationLogs: id:%d fileInfo:%s\n", fi, f.toString().c_str());
			beginBlock = 0;
			j = beginBlock *f.blockSize;
			readLen = 0;
			// For each block of the file
			for(; j < f.fileSize; j += f.blockSize) {
				readOffset = j;
				readLen = std::min<int64_t>(f.blockSize, f.fileSize - j);
				printf("ApplyMutationLogs: id:%d fileInfo:%s, readOffset:%d\n", fi, f.toString().c_str(), readOffset);

				//futures.push_back(_executeApplyMutationLogFileToDB(cx, task, f, readOffset, readLen, bc, restoreRange, addPrefix, removePrefix));
				wait( _executeApplyMutationLogFileToDB(cx, task, f, readOffset, readLen, bc, restoreRange, addPrefix, removePrefix) );

				// Increment beginBlock for the file
				++beginBlock;
				TraceEvent("ApplyLogFileToDB_MX_Offset").detail("FileInfo", f.toString()).detail("ReadOffset", readOffset).detail("ReadLen", readLen);
			}

//			_executeApplyMutationLogFileToDB(cx, task, f, 0, f.fileSize, bc, restoreRange, addPrefix, removePrefix);
			//_finishApplyMutationLogFileToDB(cx, task);
			//TraceEvent("ApplyLogFileToDB_MX").detail("KVOpsMapSizeMX", kvOps.size());
		}
	}
	printf("Wait for  futures of concatenate mutation logs, start waiting\n");
//	wait(waitForAll(futures));
	printf("Wait for  futures of concatenate mutation logs, finish waiting\n");

	printf("Now parse concatenated mutation log and register it to kvOps, start...\n");
	registerBackupMutationForAll(Version());
	printf("Now parse concatenated mutation log and register it to kvOps, done...\n");

	//Get the range file into the kvOps later
	printf("ApplyRangeFiles\n");
	futures.clear();
	for ( fi = 0; fi < files.size(); ++fi ) {
		f = files[fi];
		printf("ApplyRangeFiles:id:%d\n", fi);
		if ( f.isRange ) {
			TraceEvent("ApplyRangeFileToDB_MX").detail("FileInfo", f.toString());
			printf("ApplyRangeFileToDB_MX FileInfo:%s\n", f.toString().c_str());
			beginBlock = 0;
			j = beginBlock *f.blockSize;
			readLen = 0;
			// For each block of the file
			for(; j < f.fileSize; j += f.blockSize) {
				readOffset = j;
				readLen = std::min<int64_t>(f.blockSize, f.fileSize - j);
				futures.push_back( _executeApplyRangeFileToDB(cx, restore, task, f, readOffset, readLen, bc, restoreRange, addPrefix, removePrefix) );

				// Increment beginBlock for the file
				++beginBlock;
				TraceEvent("ApplyRangeFileToDB_MX").detail("FileInfo", f.toString()).detail("ReadOffset", readOffset).detail("ReadLen", readLen);
			}


			// Reference<IBackupContainer> bc, KeyRange restoreRange, Key addPrefix, Key removePrefix
			//Reference<IBackupContainer> bcmx = bc;
//			_executeApplyRangeFileToDB(cx, restore, task, f, 0, f.fileSize, bc, restoreRange, addPrefix, removePrefix);
			//TraceEvent("ApplyRangeFileToDB_MX").detail("KVOpsMapSizeMX", kvOps.size());
			//_finishApplyRangeFileToDB(cx, task);
		}
	}
	if ( futures.size() != 0 ) {
		printf("Wait for  futures of applyRangeFiles, start waiting\n");
		wait(waitForAll(futures));
		printf("Wait for  futures of applyRangeFiles, finish waiting\n");
	}

//	printf("Now print KVOps\n");
//	printKVOps();

//	printf("Now sort KVOps in increasing order of commit version\n");
//	sort(kvOps.begin(), kvOps.end()); //sort in increasing order of key using default less_than comparator
	if ( isKVOpsSorted() ) {
		printf("[CORRECT] KVOps is sorted by version\n");
	} else {
		printf("[ERROR]!!! KVOps is NOT sorted by version\n");
//		assert( 0 );
	}

	if ( allOpsAreKnown() ) {
		printf("[CORRECT] KVOps all operations are known.\n");
	} else {
		printf("[ERROR]!!! KVOps has unknown mutation op. Exit...\n");
//		assert( 0 );
	}

	printf("Now apply KVOps to DB. start...\n");
	wait( applyKVOpsToDB(cx) );
	printf("Now apply KVOps to DB, Done\n");
//	filterAndSortMutationOps();


	//TODO: Apply the kv operations

	return Void();
}


ACTOR static Future<Void> submitRestoreMX(Database cx, Reference<ReadYourWritesTransaction> tr, Key tagName, Key backupURL, Version restoreVersion, Key addPrefix, Key removePrefix, KeyRange restoreRange, bool lockDB, UID uid,
		Reference<Task> task) {
	ASSERT(restoreRange.contains(removePrefix) || removePrefix.size() == 0);

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	// Get old restore config for this tag
	state KeyBackedTag tag = makeRestoreTag(tagName.toString());
	state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
	TraceEvent("SubmitRestoreMX").detail("OldUidAndAbortedPresent", oldUidAndAborted.present());
	if(oldUidAndAborted.present()) {
		if (oldUidAndAborted.get().first == uid) {
			if (oldUidAndAborted.get().second) {
				throw restore_duplicate_uid();
			}
			else {
				return Void();
			}
		}

		state RestoreConfig oldRestore(oldUidAndAborted.get().first);

		// Make sure old restore for this tag is not runnable
		bool runnable = wait(oldRestore.isRunnable(tr));

		if (runnable) {
			throw restore_duplicate_tag();
		}

		// Clear the old restore config
		oldRestore.clear(tr);
	}

	KeyRange restoreIntoRange = KeyRangeRef(restoreRange.begin, restoreRange.end).removePrefix(removePrefix).withPrefix(addPrefix);
	Standalone<RangeResultRef> existingRows = wait(tr->getRange(restoreIntoRange, 1));
	if (existingRows.size() > 0) {
		throw restore_destination_not_empty();
	}

	// Make new restore config
	state RestoreConfig restore(uid);

	// Point the tag to the new uid
	tag.set(tr, {uid, false});

	Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupURL.toString());

	// Configure the new restore
	restore.tag().set(tr, tagName.toString());
	restore.sourceContainer().set(tr, bc);
	restore.stateEnum().set(tr, ERestoreState::QUEUED);
	restore.restoreVersion().set(tr, restoreVersion);
	restore.restoreRange().set(tr, restoreRange);
	// this also sets restore.add/removePrefix.
	restore.initApplyMutations(tr, addPrefix, removePrefix);
	TraceEvent("SubmitRestoreMX").detail("RestoreConfigConstruct", "Done");
	wait(restore.toTask(tr, task));

	// MX: no need to add task. Instead, we should directly run the execute function
	//Key taskKey = wait(fileBackup::StartFullRestoreTaskFunc::addTask(tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

	if (lockDB)
		wait(lockDatabase(tr, uid));
	else
		wait(checkDatabaseLock(tr, uid));


	return Void();
}


ACTOR static Future<Void> _finishMX(Reference<ReadYourWritesTransaction> tr,  Reference<Task> task,  UID uid) {
//	wait(checkTaskVersion(tr->getDatabase(), task, name, version));

	//state RestoreConfig restore(task);
	state RestoreConfig restore(uid);
//	restore.stateEnum().set(tr, ERestoreState::COMPLETED);
	// Clear the file map now since it could be huge.
//	restore.fileSet().clear(tr);

	// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for any restore operation
	// the ranges to restore must be within the backed up ranges, otherwise from the restore perspective it will appear that some
	// key ranges were missing and so the backup set is incomplete and the restore has failed.
	// This validation cannot be done currently because Restore only supports a single restore range but backups can have many ranges.

	// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored version.
//	restore.clearApplyMutationsKeys(tr);

//	wait(taskBucket->finish(tr, task));


	try {
		printf("UnlockDB now. Start.\n");
		wait(unlockDatabase(tr, uid)); //NOTE: unlockDatabase didn't commit inside the function!

		printf("CheckDBlock:%s START\n", uid.toString().c_str());
		wait(checkDatabaseLock(tr, uid));
		printf("CheckDBlock:%s DONE\n", uid.toString().c_str());

		printf("UnlockDB now. Commit.\n");
		wait( tr->commit() );

		printf("UnlockDB now. Done.\n");
	} catch( Error &e ) {
		printf("Error when we unlockDB. Error:%s\n", e.what());
		wait(tr->onError(e));
	}




	return Void();
}



ACTOR static Future<Version> restoreMX(Database cx, RestoreRequest request) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange range = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;

	//Future<Void> restoreAgentFuture1 = restoreAgent_run(cx.getPtr()->cluster->getConnectionFile(), LocalityData());
	//Future<Void> restoreAgentFuture2 = restoreAgent_run(cx.getPtr()->cluster->getConnectionFile(), LocalityData());
	//TraceEvent("WaitOnRestoreAgentFutureBegin").detail("URL", url.contents().printable());
	//wait(restoreAgentFuture1 || restoreAgentFuture2);
	//TraceEvent("WaitOnRestoreAgentFutureEnd").detail("URL", url.contents().printable());

	state Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString());
	state BackupDescription desc = wait(bc->describeBackup());

	wait(desc.resolveVersionTimes(cx));

	printf("Backup Description\n%s", desc.toString().c_str());
	printf("MX: Restore code comes here in restoreMX(), lockDB:%d\n", lockDB);
	if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		targetVersion = desc.maxRestorableVersion.get();

	Optional<RestorableFileSet> restoreSet = wait(bc->getRestoreSet(targetVersion));

	//Above is the restore master code
	//Below is the agent code
	TraceEvent("RestoreMX").detail("StartRestoreForRequest", request.toString());

	if(!restoreSet.present()) {
		TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
				.detail("BackupContainer", bc->getURL())
				.detail("TargetVersion", targetVersion);
		fprintf(stderr, "ERROR: Restore version %lld is not possible from %s\n", targetVersion, bc->getURL().c_str());
		throw restore_invalid_version();
	}

	if (verbose) {
		printf("Restoring backup to version: %lld\n", (long long) targetVersion);
		TraceEvent("RestoreBackupMX").detail("TargetVersion", (long long) targetVersion);
	}

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state Reference<Task> task(new Task());
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(submitRestoreMX(cx, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid, task));

			state RestoreConfig restore(task);
			TraceEvent("SetApplyEndVersion_MX").detail("TargetVersion", targetVersion);
			restore.setApplyEndVersion(tr, targetVersion); //MX: TODO: This may need to be set at correct position and may be set multiple times?

			wait(tr->commit());
			// MX: Now execute the restore: Step 1 get the restore files (range and mutation log) name
			wait( _executeMX(cx, task, randomUid, request) );
			printf("Finish my restore now!\n");

			//Unlock DB
			TraceEvent("RestoreMX").detail("UnlockDB", "Start");
			//state RestoreConfig restore(task);

			// MX: Unlock DB after restore
			state Reference<ReadYourWritesTransaction> tr_unlockDB(new ReadYourWritesTransaction(cx));
			printf("Finish restore cleanup. Start\n");
			wait( _finishMX(tr_unlockDB, task, randomUid) );
			printf("Finish restore cleanup. Done\n");

			TraceEvent("RestoreMX").detail("UnlockDB", "Done");



			break;
		} catch(Error &e) {
			if(e.code() != error_code_restore_duplicate_tag) {
				wait(tr->onError(e));
			}
		}
	}
//
//	state RestoreConfig restore(task);
//	state Reference<ReadYourWritesTransaction> tr1(new ReadYourWritesTransaction(cx));
//	tr1->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
//	tr1->setOption(FDBTransactionOptions::LOCK_AWARE);
//
//	TraceEvent("Restore_MX").detail("SetApplyEndVersion", "ApplyMutationsEnd").detail("TargetVersion", targetVersion);
//	restore.setApplyEndVersion(tr1, targetVersion); //MX: TODO: This may need to be set at correct position and may be set multiple times?
//	wait(tr1->commit());



	//TODO: _finish() task: Make sure the restore is finished.

	//TODO: Uncomment the following code later
//	if(waitForComplete) {
//		ERestoreState finalState = wait(waitRestore(cx, tagName, verbose));
//		if(finalState != ERestoreState::COMPLETED)
//			throw restore_error();
//	}

	return targetVersion;
}

/*
ACTOR static Future<Version> restoreSequentialMX(Database cx, RestoreRequest request) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange range = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;

	state Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString());
	state BackupDescription desc = wait(bc->describeBackup());

	wait(desc.resolveVersionTimes(cx));

	printf("Backup Description\n%s", desc.toString().c_str());
	printf("MX: Restore code comes here\n");
	if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		targetVersion = desc.maxRestorableVersion.get();

	Optional<RestorableFileSet> restoreSet = wait(bc->getRestoreSet(targetVersion));

	TraceEvent("RestoreMX").detail("StartRestoreForRequest", request.toString());

	if(!restoreSet.present()) {
		TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
				.detail("BackupContainer", bc->getURL())
				.detail("TargetVersion", targetVersion);
		fprintf(stderr, "ERROR: Restore version %lld is not possible from %s\n", targetVersion, bc->getURL().c_str());
		throw restore_invalid_version();
	}

	if (verbose) {
		printf("Restoring backup to version: %lld\n", (long long) targetVersion);
		TraceEvent("RestoreBackupMX").detail("TargetVersion", (long long) targetVersion);
	}

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state Reference<Task> task(new Task());
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			//wait(submitRestoreMX(cx, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid, task));
			// ------------START 	wait(submitRestoreMX(cx, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid, task));
			ASSERT(restoreRange.contains(removePrefix) || removePrefix.size() == 0);
			// Get old restore config for this tag
			state KeyBackedTag tag = makeRestoreTag(tagName.toString());
			state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
			TraceEvent("SubmitRestoreMX").detail("OldUidAndAbortedPresent", oldUidAndAborted.present());
			if(oldUidAndAborted.present()) {
				if (oldUidAndAborted.get().first == uid) {
					if (oldUidAndAborted.get().second) {
						throw restore_duplicate_uid();
					}
					else {
						return Void();
					}
				}

				state RestoreConfig oldRestore(oldUidAndAborted.get().first);

				// Make sure old restore for this tag is not runnable
				bool runnable = wait(oldRestore.isRunnable(tr));

				if (runnable) {
				throw restore_duplicate_tag();
				}

				// Clear the old restore config
				oldRestore.clear(tr);
			}

			KeyRange restoreIntoRange = KeyRangeRef(restoreRange.begin, restoreRange.end).removePrefix(removePrefix).withPrefix(addPrefix);
			Standalone<RangeResultRef> existingRows = wait(tr->getRange(restoreIntoRange, 1));
			if (existingRows.size() > 0) {
				throw restore_destination_not_empty();
			}

			// Make new restore config
			state RestoreConfig restore(uid);

			// Point the tag to the new uid
			tag.set(tr, {uid, false});

			Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupURL.toString());

			// Configure the new restore
			restore.tag().set(tr, tagName.toString());
			restore.sourceContainer().set(tr, bc);
			restore.stateEnum().set(tr, ERestoreState::QUEUED);
			restore.restoreVersion().set(tr, restoreVersion);
			restore.restoreRange().set(tr, restoreRange);
			// this also sets restore.add/removePrefix.
			restore.initApplyMutations(tr, addPrefix, removePrefix);
			TraceEvent("SubmitRestoreMX").detail("RestoreConfigConstruct", "Done");
			wait(restore.toTask(tr, task));

			// MX: no need to add task. Instead, we should directly run the execute function
			//Key taskKey = wait(fileBackup::StartFullRestoreTaskFunc::addTask(tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

			if (lockDB)
				wait(lockDatabase(tr, uid));
			else
				wait(checkDatabaseLock(tr, uid));

			// -------------END


			//state RestoreConfig restore(task);
			//TraceEvent("SetApplyEndVersion_MX").detail("TargetVersion", targetVersion);
			//restore.setApplyEndVersion(tr, targetVersion); //MX: TODO: This may need to be set at correct position and may be set multiple times?

			wait(tr->commit());
			// MX: Now execute the restore: Step 1 get the restore files (range and mutation log) name
			wait( _executeMX(cx, task) );
			break;
		} catch(Error &e) {
			if(e.code() != error_code_restore_duplicate_tag) {
				wait(tr->onError(e));
			}
		}
	}
//
//	state RestoreConfig restore(task);
//	state Reference<ReadYourWritesTransaction> tr1(new ReadYourWritesTransaction(cx));
//	tr1->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
//	tr1->setOption(FDBTransactionOptions::LOCK_AWARE);
//
//	TraceEvent("Restore_MX").detail("SetApplyEndVersion", "ApplyMutationsEnd").detail("TargetVersion", targetVersion);
//	restore.setApplyEndVersion(tr1, targetVersion); //MX: TODO: This may need to be set at correct position and may be set multiple times?
//	wait(tr1->commit());

	TraceEvent("RestoreMX").detail("UnlockDB", "Start");
	//state RestoreConfig restore(task);

	// MX: Unlock DB after restore
	wait( _finishMX(tr, task) );

	//TODO: _finish() task: Make sure the restore is finished.

	//TODO: Uncomment the following code later
//	if(waitForComplete) {
//		ERestoreState finalState = wait(waitRestore(cx, tagName, verbose));
//		if(finalState != ERestoreState::COMPLETED)
//			throw restore_error();
//	}

	return targetVersion;
}

*/
