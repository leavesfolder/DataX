package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column;
public class MongoUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(MongoUtil.class);

    public static MongoClient initMongoClient(Configuration conf) {
        List addressList = conf.getList("address");
        if (addressList != null && addressList.size() > 0) {
            try {
                return new MongoClient(parseServerAddress(addressList));
            } catch (UnknownHostException var3) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
            } catch (NumberFormatException var4) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
            } catch (Exception var5) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
            }
        } else {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
    }

    public static MongoClient initCredentialMongoClient(Configuration conf, String userName, String password, String database) {
        List addressList = conf.getList("address");
        if (!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        } else {
            try {


                String e = org.apache.commons.lang3.StringUtils.join(addressList.toArray(), ",");

//
                try {
                    LOG.info("采用new MongoClientURI方式获取MongoClient");

                    String e2 = "mongodb://" + userName + ":" + password + "@" + e + "/?authMechanism=SCRAM-SHA-1";
                    MongoClientURI credential2 = new MongoClientURI(e2);
                    MongoClient mongoClient1 = new MongoClient(credential2);


                    MongoDatabase db = mongoClient1.getDatabase(database);
                    MongoIterable iterable = db.listCollectionNames();
                    MongoCursor var11 = iterable.iterator();

                    while (var11.hasNext()) {
                        String collectionname = (String) var11.next();
                        LOG.debug("该库名下的表有:" + collectionname);
                        System.out.println();
                    }

                    return mongoClient1;
                } catch (Exception e14) {
                    LOG.error("85",e14);
                }


                try {
                    LOG.info("采用new MongoClientURI方式获取MongoClient,授权库不是admin");

                    // 进行转义
                    password = password.replace("%", "%25");
                    String e2 = "mongodb://" + userName + ":" + password + "@" + e + "/" + database;
                    LOG.debug("uri is {}", e2);


//                    MongoClientOptions.Builder builder = new MongoClientOptions.Builder()
//                            .socketTimeout(600000)
//                            .connectTimeout(600000)
//                            .maxConnectionIdleTime(600000)
//                            .maxConnectionLifeTime(600000)
//                            .maxWaitTime(600000)
//                            .heartbeatConnectTimeout(600000)
//                            .heartbeatSocketTimeout(600000)
//                            .serverSelectionTimeout(600000);
                    MongoClientURI credential2 = new MongoClientURI(e2);
//                    MongoClientURI credential2 = new MongoClientURI(e2, builder);


//
                    MongoClient mongoClient1 = new MongoClient(credential2);
                    MongoDatabase db = mongoClient1.getDatabase(database);
                    MongoIterable iterable = db.listCollectionNames();
                    MongoCursor var11 = iterable.iterator();

                    while (var11.hasNext()) {
                        String collectionname = (String) var11.next();
                        LOG.debug("该库名下的表有:" + collectionname);
                    }


                    return mongoClient1;
                } catch (Exception e14) {
                    LOG.error("112",e14);
                }


//
//


                LOG.info("采用new MongoClient方式获取MongoClient");

                LOG.debug("addressList is {}", addressList);
                MongoCredential credential1 = MongoCredential.createCredential(userName, database, password.toCharArray());;
//                MongoClientOptions options = MongoClientOptions.builder()
//                        .connectTimeout(600000)
//                        .maxConnectionIdleTime(600000)
//                        .maxConnectionLifeTime(600000)
//                        .maxWaitTime(600000)
//                        .serverSelectionTimeout(600000)
//                        .socketTimeout(600000)
//                        .build();

//                MongoClient mongoClient = new MongoClient(parseServerAddress(addressList), Arrays.asList(new MongoCredential[]{credential1}),options);
                MongoClient mongoClient = new MongoClient(parseServerAddress(addressList), Arrays.asList(new MongoCredential[]{credential1}));

                MongoDatabase db = mongoClient.getDatabase(database);
                MongoIterable iterable = db.listCollectionNames();
                MongoCursor var11 = iterable.iterator();

                while (var11.hasNext()) {
                    String collectionname = (String) var11.next();
                    LOG.debug("该库名下的表有:" + collectionname);
                    System.out.println();
                }


                return mongoClient;
//


            } catch (UnknownHostException var14) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
            } catch (NumberFormatException var15) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
            } catch (Exception var16) {
                LOG.error("158未知异常", var16);
                throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
            }
        }
    }

    private static boolean isHostPortPattern(List addressList) {
        Iterator var1 = addressList.iterator();

        Object address;
        String regex;
        do {
            if (!var1.hasNext()) {
                return true;
            }

            address = var1.next();
            regex = "(\\S+):([0-9]+)";
        } while (((String) address).matches(regex));

        return false;
    }

    private static List parseServerAddress(List<Object> rawAddressList) throws UnknownHostException {
        ArrayList addressList = new ArrayList();
        Iterator var2 = rawAddressList.iterator();
        for(Object  address:rawAddressList){
            String str = (String)address;
            String[] tempAddress = ((String) address).split(":");
           for(String temp :tempAddress){
               LOG.debug("address {},temp {}",address,temp);
           }
            ServerAddress e = new ServerAddress(tempAddress[0], Integer.parseInt(tempAddress[1]));
            addressList.add(e);
        }


        return addressList;
    }
}
