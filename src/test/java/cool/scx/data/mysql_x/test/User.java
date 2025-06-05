package cool.scx.data.mysql_x.test;

import cool.scx.data.mysql_x.annotation.Collection;

import java.time.LocalDateTime;

@Collection(prefix = "scx_dao_mysql_x")
public class User {

    public Long id;

    public String name;
    public Integer age;
    public LocalDateTime createDate;
    public UserInfo userInfo;

    public String[] tags;

    public static class UserInfo {
        public String email;
    }

}
