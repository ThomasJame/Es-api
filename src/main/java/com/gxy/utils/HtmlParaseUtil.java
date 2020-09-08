package com.gxy.utils;

import com.gxy.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component//工具类
public class HtmlParaseUtil {
    public static void main(String[] args) throws Exception {
       new  HtmlParaseUtil().parseJD("java").forEach(System.out::println);
    }
    public List<Content> parseJD(String keywords) throws Exception {
        ////获取请求 https://search.jd.com/Search?keyword=java
        String url="https://search.jd.com/Search?keyword="+keywords;
        //解析网页返回document 对象
        Document document = Jsoup.parse(new URL(url), 30000);
        Element element = document.getElementById("J_goodsList");
//        System.out.println(element);
        ///获取所有的LI元素
        Elements elements = element.getElementsByTag("li");
        ArrayList<Content> goodslist = new ArrayList<>();
        ////获取元素中的内容
        for (Element el: elements) {
            //图片是延迟加载的 source-data-lazy-img
            String img = el.getElementsByTag("img").eq(0).attr("src");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();
//            System.out.println("===========================");
//            System.out.println(img);
//            System.out.println(price);
//            System.out.println(title);
            Content content=new Content();
            content.setTitle(title);
            content.setPrice(price);
            content.setImg(img);
            goodslist.add(content);
        }
        return goodslist;
    }
}
