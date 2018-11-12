package com.aotain.statmange.dao;


import com.aotain.common.config.annotation.MyBatisDao;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;

import java.util.Map;

@MyBatisDao
public interface ZfDictChinaareaDao {


    @Select({"select area_short from zf_dict_chinaarea where area_code=#{area_code}"})
    String select_zf_dict_chinaarea(Map<String, Object> map);

}
