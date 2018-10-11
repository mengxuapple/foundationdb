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
#include "flow/actorcompiler.h"  // This must be the last #include.

//Future<Void> restoreAgentDB(Database cx, LocalityData const& locality)
ACTOR Future<Void>  restoreAgentDB(Database cx_input, LocalityData locality) {
	//Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1);
	//state Database cx = wait(cluster->createDatabase(locality));
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
//
//
//ACTOR Future<Void> restoreAgent(Reference<ClusterConnectionFile> ccf, LocalityData locality) {
//	Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1);
//	Database cx = wait(cluster->createDatabase(locality));
//
//	printf("RestoreAgentUseClusterFileInput");
//	restoreAgent_impl(cx, locality);
//
//	return Future<Void>;
//}
//
//
//ACTOR Future<Void> restoreAgent(Database &cx_input, LocalityData locality) {
//	printf("RestoreAgentUseDatabaseInput");
//	restoreAgent_impl(cx_input, locality);
//
//	return Future<Void>;
//}