package cool.scx.data.mysql_x;

import cool.scx.data.Finder;
import cool.scx.data.field_policy.FieldPolicy;
import cool.scx.data.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static cool.scx.data.mysql_x.parser.MySQLXDaoWhereParser.WHERE_PARSER;

public class MySQLXFinder<Entity> implements Finder<Entity> {

    private final MySQLXRepository<Entity> repository;
    private final Query query;
    private final FieldPolicy fieldPolicy;

    public MySQLXFinder(MySQLXRepository<Entity> repository, Query query, FieldPolicy fieldPolicy) {
        this.repository = repository;
        this.query = query;
        this.fieldPolicy = fieldPolicy;
    }

    @Override
    public List<Entity> list() {
        var whereClause = WHERE_PARSER.parse(query.getWhere());
        var findStatement = repository.collection
                .find(whereClause.expression())
                .bind(whereClause.params());
        if (query.getOffset() != null) {
            findStatement.offset(query.getOffset());
        }
        if (query.getLimit() != null) {
            findStatement.limit(query.getLimit());
        }
        var docResult = findStatement.execute();
        var dbDocs = docResult.fetchAll();
        var list = new ArrayList<Entity>();
        for (var dbDoc : dbDocs) {
            list.add(repository.toEntity(dbDoc, fieldPolicy));
        }
        return list;
    }

    @Override
    public <T> List<T> list(Class<T> resultType) {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public List<Map<String, Object>> listMap() {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public void forEach(Consumer<Entity> entityConsumer) {
        var whereClause = WHERE_PARSER.parse(query.getWhere());
        var findStatement = repository.collection
                .find(whereClause.expression())
                .bind(whereClause.params());
        if (query.getOffset() != null) {
            findStatement.offset(query.getOffset());
        }
        if (query.getLimit() != null) {
            findStatement.limit(query.getLimit());
        }
        var docResult = findStatement.execute();
        for (var dbDoc : docResult) {
            entityConsumer.accept(repository.toEntity(dbDoc, fieldPolicy));
        }
    }

    @Override
    public <T> void forEach(Consumer<T> entityConsumer, Class<T> resultType) {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public void forEachMap(Consumer<Map<String, Object>> entityConsumer) {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public Entity first() {
        var whereClause = WHERE_PARSER.parse(query.getWhere());
        var findStatement = repository.collection
                .find(whereClause.expression())
                .bind(whereClause.params())
                .limit(1);

        var docResult = findStatement.execute();
        var dbDoc = docResult.fetchOne();

        return repository.toEntity(dbDoc, fieldPolicy);
    }

    @Override
    public <T> T first(Class<T> resultType) {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public Map<String, Object> firstMap() {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public long count() {
        var whereClause = WHERE_PARSER.parse(query.getWhere());
        var docResult = repository.collection
                .find(whereClause.expression())
                .bind(whereClause.params())
                .execute();
        return docResult.count();
    }

}
