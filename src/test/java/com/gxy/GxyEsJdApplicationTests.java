package com.gxy;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

////从demo那边复制来的

@SpringBootTest
class GxyEsJdApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //索引创建
    @Test
    void testCreateIndex() throws IOException {
        //1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("gxy_index");
        //2.执行 返回IndicesClient
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);

    }

    //获取索引 判断存不存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("gxy_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("gxy_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());

    }

    ///////////////////文档////////////////
    @Test
    void testAddDocument() throws IOException {
        User user = new User("gxy", 3);
        IndexRequest request = new IndexRequest("gxy_index");
        ///规则
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //将数据放入请求json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        ///客户端发送请求 获取响应结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());
    }

    /////获取文档和是否存在
    @Test
    void testIsExists() throws IOException {

        GetRequest getRequest = new GetRequest("gxy_index", "1");
//        获取文档
        //不获取_source的上下文
//        getRequest.fetchSourceContext(new FetchSourceContext(false));
//        getRequest.storedFields("_none_");///排序字段
//        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
//        返回文档
        RequestOptions options = RequestOptions.DEFAULT;
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse);
    }

    ////////更新文档信息
    @Test
    void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("gxy_index", "1");
        updateRequest.timeout("1s");
        User user = new User("gxy11111", 22);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    ////删除文档记录
    @Test
    void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest("gxy_index", "1");
        request.timeout("1s");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);

        System.out.println(delete.status());
    }

    /////批量插入
    @Test
    void testBulk() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("gxy1", 3));
        userList.add(new User("gxy2", 3));
        userList.add(new User("gxy3", 3));
        userList.add(new User("gxy4", 3));
        userList.add(new User("gxy5", 3));
        userList.add(new User("gxy6", 3));
        ///批处理请求
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(new IndexRequest("gxy_index").id("" + i + 1)
                    .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());///false 表示成功
    }

    /////查询
    @Test
    void testSearch() throws IOException {
        ////1.构建请求
        SearchRequest searchRequest = new SearchRequest(ESconst.ES_INDEX);
        //构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //设置高亮
//        searchSourceBuilder.highlighter()
        ///使用不同Builders实现不用匹配
        ///可以使用QueryBuilders 精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "gxy1");
        //匹配所有
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(termQueryBuilder);
        //分页
//        searchSourceBuilder.from();
//        searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        ///2.放入请求
        searchRequest.source(searchSourceBuilder);
        ///3.服务器执行请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits() ));
        System.out.println("=======================");
        for (SearchHit documentFields:searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }


    }
}
