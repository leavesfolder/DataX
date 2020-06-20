package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.lang.Math.toIntExact;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class CollectionSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(CollectionSplitUtil.class);



    private boolean isObjectId;

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient,String query) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId,query);
        for(Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }


    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {

        MongoDatabase database = mongoClient.getDatabase(dbName);



        MongoCollection<Document> col = database.getCollection(collName);
        MongoCursor<Document> cursor = col.find().limit(1).iterator();
        while(cursor.hasNext()){
            LOG.debug("获取到的数据为:"+cursor.next().toJson());
        }
        Document doc = col.find().limit(1).first();
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        LOG.debug("获取到的id为:"+id.toString());
        if (id instanceof ObjectId) {
            LOG.debug("是ObjectId");
            return true;
        }
        return false;
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId,String query) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            LOG.info("完成切片任务");

            return Arrays.asList(range);
        }

        Document result = database.runCommand(new Document("collStats", collName));
        // Some collections return double value
        Object countObject = result.get("count");
        Long docCount = 0L;

        if(countObject instanceof Integer){
            docCount = new Long((Integer)countObject);
        }else if(countObject instanceof Double){
            docCount = ((Double)countObject).longValue();
            if(!new Double(docCount).equals((Double)countObject)){
                String message = "Double cast to Long is Error:"+(Double)countObject;
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
            }
        }





//        int docCount = result.getInteger("count");
        if (docCount == 0) {
            return rangeList;
        }
        int avgObjSize = 1;
        Object avgObjSizeObj = result.get("avgObjSize");
        if (avgObjSizeObj instanceof Integer) {
            avgObjSize = ((Integer) avgObjSizeObj).intValue();
        } else if (avgObjSizeObj instanceof Double) {
            avgObjSize = ((Double) avgObjSizeObj).intValue();
        }
        int splitPointCount = adviceNumber - 1;
        long expectChunkDocCount = docCount / adviceNumber;
        int chunkDocCount=0;
        if((int)expectChunkDocCount != expectChunkDocCount){
            String message = "The split has too many records :"+expectChunkDocCount+". Please let the \"job.setting.speed.channel\" parameter exceed  "+adviceNumber;
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
        }else{
            chunkDocCount = (int)expectChunkDocCount;

        }

//        int chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<Object>();

        // test if user has splitVector role(clusterManager)
        boolean supportSplitVector = true;
        try {
            database.runCommand(new Document("splitVector", dbName + "." + collName)
                    .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                    .append("force", true));
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == KeyConstant.MONGO_UNAUTHORIZED_ERR_CODE ||
                    e.getErrorCode() == KeyConstant.MONGO_ILLEGALOP_ERR_CODE) {
                supportSplitVector = false;
            }
        }

        if (supportSplitVector) {
            boolean forceMedianSplit = false;
            long maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
//            int maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
            //int maxChunkSize = (chunkDocCount - 1) * 2 * avgObjSize / (1024 * 1024);
            if (maxChunkSize < 1) {
                forceMedianSplit = true;
            }
            if (!forceMedianSplit) {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("maxChunkSize", maxChunkSize)
                        .append("maxSplitPoints", adviceNumber - 1));
            } else {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("force", true));
            }
            ArrayList<Document> splitKeys = result.get("splitKeys", ArrayList.class);

            for (int i = 0; i < splitKeys.size(); i++) {
                Document splitKey = splitKeys.get(i);
                Object id = splitKey.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId)id;
                    splitPoints.add(oid.toHexString());
                } else {
                    splitPoints.add(id);
                }
            }
        } else {
            int skipCount = chunkDocCount;
            MongoCollection<Document> col = database.getCollection(collName);


            Document filter = new Document();
            if(!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }

            Document firstDoc = col.find(filter).sort(Sorts.orderBy(Sorts.ascending(KeyConstant.MONGO_PRIMARY_ID))).first();
            Document lastDoc = col.find(filter).sort(Sorts.orderBy(Sorts.descending(KeyConstant.MONGO_PRIMARY_ID))).first();
            if (null==firstDoc) {

                Range range = new Range();
                range.lowerBound = "min";
                range.upperBound = "max";
                return Arrays.asList(range);
            }
            ObjectId objectId = firstDoc.getObjectId(KeyConstant.MONGO_PRIMARY_ID);


            // 将最大的一条记录的时间戳值+1秒钟，避免漏掉最大的一条记录

            splitPoints = split(adviceNumber,Integer.MAX_VALUE,firstDoc.getObjectId(KeyConstant.MONGO_PRIMARY_ID).getTimestamp(), lastDoc.getObjectId(KeyConstant.MONGO_PRIMARY_ID).getTimestamp()+1 );
        }

        Object lastObjectId = "";

        for(int i=0;i<splitPoints.size();i++){

            String splitPoint = timeStampToObjectId((Integer)splitPoints.get(i));

            if(i==0){
                lastObjectId = splitPoint;
                continue;
            }

            Range range = new Range();
            range.lowerBound = lastObjectId;
            range.upperBound = splitPoint;

            lastObjectId = splitPoint;
            rangeList.add(range);
        }


        for(Range thisRange:rangeList){
            LOG.info("切片为："+thisRange.lowerBound+"  "+thisRange.upperBound);
        }

        return rangeList;
    }

    public static String timeStampToObjectId(int time){
        String objectId = Long.toHexString(time);
        while(objectId.length()<24){
            objectId=objectId+"0";
        }
        return objectId;

    }



    /**
     *从org.apache.sqoop.mapreduce.db.IntegerSplitter类中粘贴过来
     *
     * @param numSplits Number of split chunks.
     * @param splitLimit Limit the split size.
     * @param minVal Minimum value of the set to split.
     * @param maxVal Maximum value of the set to split.
     * @return Split values inside the set.
     * @throws SQLException In case of SQL exception.
     */
    private  static ArrayList<Object> split(int numSplits,int splitLimit, int minVal, int maxVal)
    {

        ArrayList<Object> splits = new ArrayList<Object>();

        // We take the min-max interval and divide by the numSplits and also
        // calculate a remainder.  Because of integer division rules, numsplits *
        // splitSize + minVal will always be <= maxVal.  We then use the remainder
        // and add 1 if the current split index is less than the < the remainder.
        // This is guaranteed to add up to remainder and not surpass the value.
        int splitSize = (maxVal - minVal) / numSplits;
        double splitSizeDouble = ((double)maxVal - (double)minVal) / (double)numSplits;

        if (splitLimit > 0 && splitSizeDouble > splitLimit) {
            // If split size is greater than limit then do the same thing with larger
            // amount of splits.
            int newSplits = (maxVal - minVal) / splitLimit;
            return split(newSplits != numSplits ? newSplits : newSplits + 1,
                    splitLimit, minVal, maxVal);
        }

        int remainder = (maxVal - minVal) % numSplits;
        int curVal = minVal;

        // This will honor numSplits as long as split size > 0.  If split size is
        // 0, it will have remainder splits.
        for (int i = 0; i <= numSplits; i++) {
            splits.add(curVal);
            if (curVal >= maxVal) {
                break;
            }
            curVal += splitSize;
            curVal += (i < remainder) ? 1 : 0;
        }

        if (splits.size() == 1) {
            // make a valid singleton split
            splits.add(maxVal);
        } else if ((maxVal - minVal) <= numSplits) {
            // Edge case when there is lesser split points (intervals) then
            // requested number of splits. In such case we are creating last split
            // with two values, for example interval [1, 5] broken down into 5
            // splits will create following conditions:
            //  * 1 <= x < 2
            //  * 2 <= x < 3
            //  * 3 <= x < 4
            //  * 4 <= x <= 5
            // Notice that the last split have twice more data than others. In
            // those cases we add one maxVal at the end to create following splits
            // instead:
            //  * 1 <= x < 2
            //  * 2 <= x < 3
            //  * 3 <= x < 4
            //  * 4 <= x < 5
            //  * 5 <= x <= 5
            splits.add(maxVal);
        }



        return splits;
    }







}

class Range {
    Object lowerBound;
    Object upperBound;

}
