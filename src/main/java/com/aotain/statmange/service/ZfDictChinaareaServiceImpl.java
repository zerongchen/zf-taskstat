package com.aotain.statmange.service;

import com.aotain.statmange.dao.ZfDictChinaareaDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ZfDictChinaareaServiceImpl {

    @Autowired
    private ZfDictChinaareaDao zfDictChinaareaDao;

    public String select_zf_dict_chinaarea(Map<String, Object> map){
        return zfDictChinaareaDao.select_zf_dict_chinaarea(map);
    }

}
