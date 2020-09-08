package com.gxy.service;

import com.alibaba.fastjson.JSON;
import com.gxy.pojo.Content;
import com.gxy.utils.HtmlParaseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.naming.directory.SearchResult;
import javax.validation.constraints.Null;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //    /////不能直接使用@Autowired 只要Spring容器
//    public static void main(String[] args) throws Exception {
//       new ContentService().parseContent("java");
//    }
    ///1.解析数据放入ES 中
    public Boolean parseContent(String keywords) throws Exception {
        List<Content> contents = new HtmlParaseUtil().parseJD(keywords);
        ///将查询数据放入ES
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");

        for (int i = 0; i < contents.size(); i++) {
            System.out.println(JSON.toJSONString(contents.get(i)));
            bulkRequest.add(new IndexRequest("jd_goods")
                    .source(JSON.toJSONString(contents.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulk.hasFailures();
    }

    ///2.获取这些数据实现搜索功能
    public List<Map<String, Object>> searchPage(String keyword, int
            page, int pagesize) throws IOException {
        if (pagesize <= 1) {
            pagesize = 1;
        }
        ///条件搜索
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        ////分页
        sourceBuilder.from(page);
        sourceBuilder.size(pagesize);
        //精准匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //高亮
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.field("title");
//        highlightBuilder.requireFieldMatch(false);////关闭多个高亮
//        highlightBuilder.preTags("<span style='color:red'");
//        highlightBuilder.postTags("</span>");
//        sourceBuilder.highlighter(highlightBuilder);
        //执行搜索
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ////解析结果
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            ///解析高亮字段
//          Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
//          HighlightField title= highlightFields.get("title");
//            Map<String, Object> sourceAsMap=documentFields.getSourceAsMap();///原来结果
//            ///将原来的字段换位高亮的字段
//            if(title!= null){
//                Text[] fragments=title.fragments();
//                String n_title="";
//                for (Text text:fragments){
//                    n_title +=text;
//                }
//                sourceAsMap.put("title",n_title);///替换
//            }
//            list.add(sourceAsMap);
            list.add(documentFields.getSourceAsMap());
        }
        return list;

    }
}
