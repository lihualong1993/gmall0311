package com.atguigu.gmall0311.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {
    public  Long getDauTotal(String date);


    public Map<String,Long> getDauHourCount(String date);
}
