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

ACTOR Future<Void> restoreAgent(Reference<ClusterConnectionFile> ccf, LocalityData locality) {
	Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1);
	state Database cx = wait(cluster->createDatabase(locality));
	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: restoreAgent starts. Try to be a leader. Locality:%s\n", locality.toString().c_str());
	state Transaction tr(cx);
	loop {
		try {
			Optional<Value> leader = wait(tr.get(restoreLeaderKey));
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
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
				}
			}
		}
	}

	//we are the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader. Locality:%s\n", locality.toString().c_str());
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
		std::vector<Future<TestReply>> replies;
		for(auto& it : agents) {
			replies.push_back( it.test.getReply(TestRequest(testData)) );
		}
		printf("Wait on all %d requests for testData %d\n", agents.size(), testData);

		std::vector<TestReply> reps = wait( getAny(replies ));
		printf("getTestReply number:%d\n", reps.size());
		testData = reps[0].replyData;
	}
}
