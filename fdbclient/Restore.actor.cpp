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

#include "RestoreInterface.h"
#include "NativeAPI.h"
#include "SystemData.h"

//Backup agent header
#include "BackupAgent.h"
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



//MX: Hack: directly copy the function above with minor change
//ACTOR Future<Void> restoreAgent_run(Database db) {
ACTOR Future<Void> restoreAgent_run(Reference<ClusterConnectionFile> ccf, LocalityData locality) {

	//TraceEvent("RestoreAgentRun");
	TraceEvent("RestoreAgentRun").detail("ClusterFile", ccf->getFilename());
	//Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1); //Cannot use filename to create cluster because the filename may not be found in the simulator when the function is invoked.
	Reference<Cluster> cluster = Cluster::createCluster(ccf, -1);
	state Database cx = wait(cluster->createDatabase(locality));

	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: RestoreAgentRun starts. Try to be a leader.\n");
	TraceEvent("RestoreAgentStartTryBeLeader");
	state Transaction tr(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr.numErrors).detail("ReadLeaderKey", restoreLeaderKey.printable());
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
		printf("MX: I am NOT the leader.\n");
		TraceEvent("RestoreAgentNotLeader");
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

	}

	//I am the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader.\n");
	TraceEvent("RestoreAgentIsLeader");
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

	//create initial point, The initial request value for agent i is i.
	state std::vector<int> restoreRequests;
	for ( int i = 0; i < agents.size(); ++i ) {
		restoreRequests.push_back(i);
	}

	loop {
		wait(delay(1.0));

		printf("---Sending Requests\n");
		TraceEvent("RestoreAgentLeader").detail("SendingRequests", "CheckBelow");
		for (int i = 0; i < restoreRequests.size(); ++i ) {
			printf("RestoreRequests[%d]=%d\n", i, restoreRequests[i]);
			TraceEvent("RestoreRequests").detail("Index", i).detail("Value", restoreRequests[i]);
		}
		std::vector<Future<RestoreReply>> replies;
		for ( int i = 0; i < agents.size(); ++i) {
			auto &it = agents[i];
			replies.push_back( it.request.getReply(RestoreRequest(i, restoreRequests)) );
		}
		printf("Wait on all %d requests\n", agents.size());

		std::vector<RestoreReply> reps = wait( getAll(replies ));
		printf("GetRestoreReply values\n", reps.size());
		for ( int i = 0; i < reps.size(); ++i ) {
			printf("RestoreReply[%d]=%d\n [checksum=%d]", i, reps[i].restoreReplies[i], reps[i].replyData);
			//prepare to send the next request batch
			restoreRequests[i] = reps[i].restoreReplies[i];
			if ( restoreRequests[i] > 10 ) { //MX: only calculate up to 10
				return Void();
			}
		}
		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
	}
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
