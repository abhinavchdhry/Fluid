package org.insight.fluid;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;

import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public final class MessageObject {

	public String id;				// ID of the message
	public String thread_id;		// ID of the thread/link where the message was posted
	public String author_id;		// ID of the author of the message

	public Integer score;			// Score of the comment
	public String parent_id;		// ID of parent comment/post
	public String subreddit_id;		// ID of the subreddit
	public String body;				// Body of the comment

	public MessageObject() {};
};
