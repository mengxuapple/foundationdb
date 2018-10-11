/*
 * RestoreInterface.h
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

#ifndef FDBCLIENT_RestoreInterface_H
#define FDBCLIENT_RestoreInterface_H
#pragma once

#include "FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

struct RestoreInterface {
	RequestStream< struct TestRequest > test;

	bool operator == (RestoreInterface const& r) const { return id() == r.id(); }
	bool operator != (RestoreInterface const& r) const { return id() != r.id(); }
	UID id() const { return test.getEndpoint().token; }
	NetworkAddress address() const { return test.getEndpoint().address; }

	void initEndpoints() {
		test.getEndpoint( TaskClusterController );
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & test;
	}
};

struct TestRequest {
	int testData;
	ReplyPromise< struct TestReply > reply;

	TestRequest() : testData(0) {}
	explicit TestRequest(int testData) : testData(testData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & testData & reply;
	}
};

struct TestReply {
	int replyData;

	TestReply() : replyData(0) {}
	explicit TestReply(int replyData) : replyData(replyData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & replyData;
	}
};

Future<Void> restoreAgent(Reference<struct ClusterConnectionFile> const& ccf, struct LocalityData const& locality);
Future<Void> restoreAgentDB(Database const& cx, LocalityData const& locality);

#endif
