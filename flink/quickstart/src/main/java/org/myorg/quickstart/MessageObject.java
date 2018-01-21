/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;

import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public final class MessageObject {

	public String id;		// ID of the message
	public String thread_id;	// ID of the thread/link where the message was posted
	public String author_id;	// ID of the author of the message

	public Integer score;		// Score of the comment
	public String parent_id;	// ID of parent comment/post
	public String subreddit_id;	// ID of the subreddit
	public String body;		// Body of the comment

	public MessageObject() {};
	public MessageObject(String _id, String _thread_id, String _author_id, Integer _score, String _parent_id, String _subreddit_id, String _body) 
	{ 
		id = _id;
		thread_id = _thread_id;
		author_id = _author_id;
		score = _score;
		parent_id = _parent_id;
		subreddit_id = _subreddit_id;
		body = _body;
	}

	public DataSet<Tuple3<String, String, String>> toDataSet() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Tuple3<String, String, String> tuple = new Tuple3<>(id, thread_id, body);
		DataSet<Tuple3<String, String, String>> dataSet = env.fromElements(tuple);
		return dataSet;
	}

	public Tuple3<String, String, String> toTuple() {
		return new Tuple3<String, String, String>(id, thread_id, body);
	}

	public String toString() {
		return new String(id + ", " + thread_id + ", " + body);
	}
};

