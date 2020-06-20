package com.alibaba.datax.plugin.reader.mongodbreader;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.Connection;
import org.bson.types.ObjectId;

import java.util.Date;

public class MyTest {
    public static void main(String[] args) {
        ObjectId objectId = new ObjectId("5ee95d000000000000000000");
        int timestamp = objectId.getTimestamp();
        System.out.println(timestamp);
    }



}
