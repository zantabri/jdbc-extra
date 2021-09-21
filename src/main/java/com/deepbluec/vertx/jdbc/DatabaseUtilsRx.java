package com.deepbluec.vertx.jdbc;

import ca.krasnay.sqlbuilder.SelectBuilder;
import ca.krasnay.sqlbuilder.SubSelectBuilder;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DatabaseUtilsRx {

    private final JDBCClient jdbcClient;
    private final Logger logger = LoggerFactory.getLogger(DatabaseUtilsRx.class.getName());


    public DatabaseUtilsRx(JDBCClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public JDBCClient getJdbcClient() {
        return this.jdbcClient;
    }

    public Single<JsonObject> updateRecord(JsonObject data) {

        StringBuilder sb = new StringBuilder("update ").append(data.getString("table")).append(" ");
        JsonObject payload = data.getJsonObject("payload");
        JsonArray ignore = data.getJsonArray("ignore");
        JsonObject criteria = data.getJsonObject("criteria");

        criteria.remove("page");
        criteria.remove("count");

        if (criteria == null || criteria.isEmpty()) {
            return Single.error(new DBQueryException(400, "update where criteria not set"));
        }

        JsonArray array = new JsonArray();
        String[] keyset = payload.getMap().keySet().toArray(new String[]{});

        for (int j = 0; j < keyset.length; j++) {

            if (ignore != null && ignore.contains(keyset[j]))
                continue;

            if (j == 0) {

                sb.append("set ").append(keyset[j]).append(" = ? ");

            } else {

                sb.append(keyset[j]).append(" = ? ");

            }

            if (j != keyset.length - 1 && keyset.length > 1) {
                sb.append(", ");
            }

            array.add(payload.getValue(keyset[j]));

        }


        String[] keys = criteria.fieldNames().toArray(new String[]{});
        sb.append(" where ");

        for (int i = 0; i < keys.length; i++) {

            if (i > 0) {
                sb.append(" and ");
            }

            sb.append(keys[i]).append(" = ? ");
            array.add(criteria.getValue(keys[i]));

        }

        return jdbcClient.rxUpdateWithParams(sb.toString(), array)
                .flatMap(ur -> Single.just(new JsonObject().put("success", true)));

    }

    public Single<JsonObject> runSearch(JsonObject queryObject) {

        String countField = queryObject.getString("countField");
        JsonArray columns = queryObject.getJsonArray("columns");

        String tableAlias = queryObject.getString("table");
        String countTableAlias = queryObject.getString("table") + "_c";

        SelectBuilder selectBuilder = new SelectBuilder();
        selectBuilder.from(queryObject.getString("table") + " " + tableAlias);

        SubSelectBuilder countSelect = new SubSelectBuilder("total");
        countSelect.from(queryObject.getString("table")  + " " + countTableAlias);
        countSelect.column("count(" + countTableAlias + "." + countField + ")");

        JsonArray selectParameters = new JsonArray();
        JsonArray countParameters = new JsonArray();

        for (int i = 0; i < columns.size(); i++) {
            selectBuilder.column( tableAlias + "." + columns.getString(i));
        }

        if(queryObject.containsKey("joins") && queryObject.getJsonArray("joins") != null) {
            JsonArray joins = queryObject.getJsonArray("joins");
            if(joins.size() > 10)
                throw new DBQueryException(500, "10 is the maximum number of joins allowed");

            for (int i = 0; i < joins.size(); i++) {

                addJoin(selectBuilder, tableAlias,joins.getJsonObject(i),0, true, selectParameters);
                addJoin(countSelect, countTableAlias, joins.getJsonObject(i), 10, false, countParameters);

            }

        }

        if(queryObject.containsKey("sort") && queryObject.getJsonObject("sort") != null) {

            JsonObject sort = queryObject.getJsonObject("sort");

            if(sort.getString("by") == null)
                throw new DBQueryException(500, "sort.by value is null");

            selectBuilder.orderBy(tableAlias + "." + sort.getString("by"), sort.getString("order", "desc").equalsIgnoreCase("asc"));

        }

        if(queryObject.containsKey("distinct") && queryObject.getBoolean("distinct")) {
            selectBuilder.distinct();
            countSelect.distinct();
        }

        addTextSearch(selectBuilder, queryObject.getString("searchTerm"), tableAlias, queryObject.getJsonArray("columns"), selectParameters);
        addTextSearch(countSelect, queryObject.getString("searchTerm"), countTableAlias, queryObject.getJsonArray("columns"), countParameters);

        selectBuilder.column(countSelect);

        logger.debug("query is {}", selectBuilder.toString());

        String count = queryObject.fieldNames().contains("count") ? queryObject.getString("count") : "10";
        String page = queryObject.fieldNames().contains("page") && Integer.valueOf(queryObject.getString("page")) > 0 ? String.valueOf(Integer.valueOf(queryObject.getString("page","1")) - 1) : "0";

        String query = new StringBuilder(selectBuilder.toString()).append(" limit :page, :count").toString().replace(":page", page).replace(":count", count);


        return jdbcClient.rxQueryWithParams(query, countParameters.addAll(selectParameters))
                .flatMap(resultSet -> {

                    JsonArray resultArray = new JsonArray(resultSet.getRows(false));
                    JsonObject results = new JsonObject().put("result", resultArray)
                            .put("count", resultArray.size())
                            .put("page", page);

                    if(!resultArray.isEmpty()) {
                        results.put("total", resultArray.getJsonObject(0).getInteger("total",resultArray.getJsonObject(0).getInteger("TOTAL")));
                    }

                    return Single.just(results);
                });
    }

    public Single<JsonObject> runPaginationQuery(JsonObject queryObject) {

        JsonObject criteria = queryObject.getJsonObject("criteria", new JsonObject());
        String countField = queryObject.getString("countField");
        JsonArray columns = queryObject.getJsonArray("columns");

        String tableAlias = queryObject.getString("table");
        String countTableAlias = queryObject.getString("table") + "_c";

        SelectBuilder selectBuilder = new SelectBuilder();
        selectBuilder.from(queryObject.getString("table") + " " + tableAlias);

        SubSelectBuilder countSelect = new SubSelectBuilder("total");
        countSelect.from(queryObject.getString("table")  + " " + countTableAlias);
        countSelect.column("count(" + countTableAlias + "." + countField + ")");

        JsonArray selectParameters = new JsonArray();
        JsonArray countParameters = new JsonArray();

        for (int i = 0; i < columns.size(); i++) {
            selectBuilder.column( tableAlias + "." + columns.getString(i));
        }

        if(queryObject.containsKey("joins") && queryObject.getJsonArray("joins") != null) {
            JsonArray joins = queryObject.getJsonArray("joins");
            if(joins.size() > 10)
                throw new DBQueryException(500, "10 is the maximum number of joins allowed");

            for (int i = 0; i < joins.size(); i++) {

                addJoin(selectBuilder, tableAlias,joins.getJsonObject(i),0, true, selectParameters);
                addJoin(countSelect, countTableAlias, joins.getJsonObject(i), 10, false, countParameters);

            }

        }

        if(queryObject.containsKey("sort") && queryObject.getJsonObject("sort") != null) {

            JsonObject sort = queryObject.getJsonObject("sort");

            if(sort.getString("by") == null)
                throw new DBQueryException(500, "sort.by value is null");

            selectBuilder.orderBy(tableAlias + "." + sort.getString("by"), sort.getString("order", "desc").equalsIgnoreCase("asc"));

        }

        if(queryObject.containsKey("distinct") && queryObject.getBoolean("distinct")) {
            selectBuilder.distinct();
            countSelect.distinct();
        }

        selectBuilder.column(countSelect);
        addCriteria(selectBuilder, criteria, tableAlias, selectParameters);
        addCriteria(countSelect, criteria, countTableAlias, countParameters);
        if(selectParameters.isEmpty()) {

            int count = queryObject.fieldNames().contains("count") ? Integer.valueOf(queryObject.getString("count")) : 10;
            int page = queryObject.fieldNames().contains("page") && Integer.valueOf(queryObject.getString("page")) > 0 ? Integer.valueOf(queryObject.getString("page","1")) - 1 : 0;



            String query = new StringBuilder(selectBuilder.toString()).append(" limit :page, :count").toString().replace(":page", String.valueOf(page * count)).replace(":count", String.valueOf(count));


            logger.debug("select query is {}", query);


            return jdbcClient.rxQuery(query)
                    .flatMap(resultSet -> {
                        JsonArray resultArray = new JsonArray(resultSet.getRows(false));
                        JsonObject results = new JsonObject().put("result", resultArray)
                                .put("count", resultArray.size())
                                .put("page", Integer.valueOf(queryObject.getString("page")));
                        if(!resultArray.isEmpty()) {
                            results.put("total", resultArray.getJsonObject(0).getInteger("total",resultArray.getJsonObject(0).getInteger("TOTAL")));
                        }
                        return Single.just(results);
                    });

        }

        int count = queryObject.fieldNames().contains("count") ? Integer.valueOf(queryObject.getString("count")) : 10;
        int page = queryObject.fieldNames().contains("page") && Integer.valueOf(queryObject.getString("page")) > 0 ? Integer.valueOf(queryObject.getString("page","1")) - 1 : 0;

        String query = new StringBuilder(selectBuilder.toString()).append(" limit :page, :count").toString().replace(":page", String.valueOf(page * count)).replace(":count", String.valueOf(count));

        logger.debug("select query is {}", query);

        logger.debug("select parameters are {}", selectParameters.toString());
        logger.debug("count parameters {}", countParameters.toString());
//        logger.debug("combined parameters {}", countParameters.addAll(selectParameters));

        return jdbcClient.rxQueryWithParams(query, countParameters.addAll(selectParameters))
                .flatMap(resultSet -> {
                    JsonArray resultArray = new JsonArray(resultSet.getRows(false));
                    JsonObject results = new JsonObject().put("result", resultArray)
                            .put("count", resultArray.size())
                            .put("page", Integer.valueOf(queryObject.getString("page", "1")));
                    if(!resultArray.isEmpty()) {
                        results.put("total", resultArray.getJsonObject(0).getInteger("total",resultArray.getJsonObject(0).getInteger("TOTAL")));
                    }
                    return Single.just(results);
                });

    }

    public Single<JsonArray> runSelectQuery(JsonObject queryObject) {

        JsonObject criteria = queryObject.getJsonObject("criteria", new JsonObject());
        JsonArray columns = queryObject.getJsonArray("columns");

        String tableAlias = queryObject.getString("table");

        SelectBuilder selectBuilder = new SelectBuilder();
        selectBuilder.from(queryObject.getString("table") + " " + tableAlias);

        JsonArray selectParameters = new JsonArray();

        for (int i = 0; i < columns.size(); i++) {
            selectBuilder.column( tableAlias + "." + columns.getString(i));
        }

        if(queryObject.containsKey("joins") && queryObject.getJsonArray("joins") != null) {
            JsonArray joins = queryObject.getJsonArray("joins");
            if(joins.size() > 10)
                throw new DBQueryException(500, "10 is the maximum number of joins allowed");

            for (int i = 0; i < joins.size(); i++) {

                addJoin(selectBuilder, tableAlias,joins.getJsonObject(i),0, true, selectParameters);

            }

        }

        if(queryObject.containsKey("sort") && queryObject.getJsonObject("sort") != null) {

            JsonObject sort = queryObject.getJsonObject("sort");

            if(sort.getString("by") == null)
                throw new DBQueryException(500, "sort.by value is null");

            selectBuilder.orderBy(tableAlias + "." + sort.getString("by"), sort.getString("order", "desc").equalsIgnoreCase("asc"));

        }

        addCriteria(selectBuilder, criteria, tableAlias, selectParameters);

        if(queryObject.containsKey("distinct") && queryObject.getBoolean("distinct")) {
            selectBuilder.distinct();
        }

        String query = new StringBuilder(selectBuilder.toString()).toString();
        logger.debug("select query is {}", query);
        if(selectParameters.isEmpty()) {

            return jdbcClient.rxQuery(query)
                    .flatMap(resultSet -> {

                        JsonArray resultArray = new JsonArray(resultSet.getRows(true));
                        return Single.just(resultArray);

                    });

        }


        return jdbcClient.rxQueryWithParams(selectBuilder.toString(), selectParameters)
                .flatMap(resultSet -> {

                    JsonArray resultArray = new JsonArray(resultSet.getRows(true));
                    return Single.just(resultArray);

                });


    }

    private SelectBuilder addJoin(SelectBuilder sb, String parentTableAlias, JsonObject join, int count, boolean includeSelect, JsonArray parameters) throws DBQueryException {


        if(join == null)
            throw new DBQueryException(500, "join object is null");

        if(join.getString("table", null) == null)
            throw new DBQueryException(500, "join table not specified");

        if(join.getValue("on", null) == null)
            throw new DBQueryException(500, "join on not specified");

        if(join.getString("type", null) == null)
            throw new DBQueryException(500, "join type not specified");

        String joinTable = join.getString("table");
        String jointTableAlias = joinTable+count;


        if(join.getValue("on") instanceof  String) {

            String  joinOn = join.getString("on");

            if(join.getString("type").equalsIgnoreCase("inner")) {

                applyInnerJoinOnStatement(sb, joinTable, jointTableAlias, parentTableAlias, joinOn, joinOn);

            } else if(join.getString("type").equalsIgnoreCase("left")) {

                applyLeftJoinOnStatement(sb, joinTable, jointTableAlias, parentTableAlias, joinOn, joinOn);

            } else {

                throw new DBQueryException(500, "join type not supported");

            }

        } else if(join.getValue("on") instanceof JsonObject) {

            if(join.getJsonObject("on", null)  != null && (join.getJsonObject("on").getString("tableColumnName") == null || join.getJsonObject("on").getString("parentColumnName") == null))
                throw new DBQueryException(500, "join incorrect");

            JsonObject  joinOn = join.getJsonObject("on");
            String tableColumnName = joinOn.getString("tableColumnName");
            String parentTableColumnName = joinOn.getString("parentTableColumnName");

            if(join.getString("type").equalsIgnoreCase("inner")) {

                applyInnerJoinOnStatement(sb, joinTable, jointTableAlias, parentTableAlias, tableColumnName, parentTableColumnName);

            } else if(join.getString("type").equalsIgnoreCase("left")) {

                applyLeftJoinOnStatement(sb, joinTable, jointTableAlias, parentTableAlias, tableColumnName, parentTableColumnName);

            } else {

                throw new DBQueryException(500, "join type not supported");

            }

        }



        if(join.containsKey("select") && join.getJsonArray("select") != null && includeSelect) {
            JsonArray select = join.getJsonArray("select");
            for (int i = 0; i < select.size(); i++) {
                sb.column(jointTableAlias + "." + select.getString(i));
            }

        }

        if(join.containsKey("criteria") && join.getJsonObject("criteria") != null)
            addCriteria(sb,join.getJsonObject("criteria"), jointTableAlias, parameters);

        if(join.containsKey("join") && join.getJsonObject("join") != null)
            return addJoin(sb, jointTableAlias, join.getJsonObject("join"),count++, includeSelect, parameters);



        return sb;

    }

    private void applyLeftJoinOnStatement(SelectBuilder sb, String joinTable, String joinTableAlias, String parenTableAlias, String tableColumnName, String parentTableColumnName) {

        sb.leftJoin(joinTable + " " + joinTableAlias + " on " + joinTableAlias + "." + tableColumnName + " =  " + parenTableAlias + "." + parentTableColumnName);

    }

    private void applyInnerJoinOnStatement(SelectBuilder sb, String joinTable, String joinTableAlias, String parenTableAlias, String tableColumnName, String parentTableColumnName) {

        sb.join(joinTable + " " + joinTableAlias + " on " + joinTableAlias + "." + tableColumnName + " =  " + parenTableAlias + "." + parentTableColumnName);

    }

    private SelectBuilder addCriteria(SelectBuilder sb, JsonObject criteria, String tableAlias, JsonArray parameters) {

        List<String> names = new ArrayList<>(criteria.fieldNames());

        for (int i = 0; i < names.size(); i++) {

            if(names.get(i).equalsIgnoreCase("from")) {

                sb.where(tableAlias + ".dateCreated" + " >= ? ");
                parameters.add(criteria.getValue(names.get(i)));

            } else if(names.get(i).equalsIgnoreCase("dateFrom") && criteria.getJsonObject("dateFrom") != null) {

                JsonObject dateFrom = criteria.getJsonObject("dateFrom");
                String name = dateFrom.getString("name");
                String value = dateFrom.getString("value");
                sb.where(tableAlias + "." + name + " >= ?");
                parameters.add(value);

            } else if(names.get(i).equalsIgnoreCase("to")) {

                sb.where(tableAlias + ".dateCreated" + " <= ?");
                parameters.add(criteria.getValue(names.get(i)));

            } else if(names.get(i).equalsIgnoreCase("dateTo") && criteria.getJsonObject("dateTo") != null) {

                JsonObject dateTo = criteria.getJsonObject("dateTo");
                String name = dateTo.getString("name");
                String value = dateTo.getString("value");
                sb.where(tableAlias + "." + name + " <= ?");
                parameters.add(value);

            } else {

                sb.where(tableAlias + "." + names.get(i) + " = ?");
                parameters.add(criteria.getValue(names.get(i)));
            }

        }

        return sb;
    }

    private SelectBuilder addTextSearch(SelectBuilder sb, String searchTerm, String tableAlias, JsonArray rows, JsonArray parameters) {

        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < rows.size(); i++) {
            stringBuilder.append(tableAlias).append(".").append(rows.getString(i));
            if( i != rows.size() - 1)
                stringBuilder.append(",");

        }
        sb.where("MATCH (" + stringBuilder.toString() + ") AGAINST (? IN NATURAL LANGUAGE MODE)");
        parameters.add(searchTerm);

        return sb;

    }

    private void applySort(SelectBuilder selectBuilder, JsonObject queryObject, String tableAlias) {
        if(queryObject.containsKey("sort") && queryObject.getJsonObject("sort") != null) {

            JsonObject sort = queryObject.getJsonObject("sort");

            if(sort.getString("by") == null)
                throw new DBQueryException(500, "sort.by value is null");

            selectBuilder.orderBy(tableAlias + "." + sort.getString("by"), sort.getString("order", "desc").equalsIgnoreCase("asc"));

        }
    }

    private void applySort(SelectBuilder selectBuilder, String by, String order, String table) {

            selectBuilder.orderBy(table + "." + by, order.equalsIgnoreCase("asc"));

    }

    public Single<JsonObject> runSingleRowQuery(JsonObject queryObject) {

        JsonObject criteria = queryObject.getJsonObject("criteria", new JsonObject());
        JsonArray columns = queryObject.getJsonArray("columns");

        String tableAlias = queryObject.getString("table");

        SelectBuilder selectBuilder = new SelectBuilder();
        selectBuilder.from(queryObject.getString("table") + " " + tableAlias);

        JsonArray selectParameters = new JsonArray();

        for (int i = 0; i < columns.size(); i++) {
            selectBuilder.column( tableAlias + "." + columns.getString(i));
        }

        if(queryObject.containsKey("joins") && queryObject.getJsonArray("joins") != null) {
            JsonArray joins = queryObject.getJsonArray("joins");
            if(joins.size() > 10)
                throw new DBQueryException(500, "10 is the maximum number of joins allowed");

            for (int i = 0; i < joins.size(); i++) {

                addJoin(selectBuilder, tableAlias,joins.getJsonObject(i),0, true, selectParameters);

            }

        }

        if(queryObject.containsKey("sort") && queryObject.getJsonObject("sort") != null) {

            JsonObject sort = queryObject.getJsonObject("sort");

            if(sort.getString("by") == null)
                throw new DBQueryException(500, "sort.by value is null");

            selectBuilder.orderBy(tableAlias + "." + sort.getString("by"), sort.getString("order", "desc").equalsIgnoreCase("asc"));

        }

        addCriteria(selectBuilder, criteria, tableAlias, selectParameters);

        if(queryObject.containsKey("distinct") && queryObject.getBoolean("distinct")) {
            selectBuilder.distinct();
        }

        String query = new StringBuilder(selectBuilder.toString()).toString();
        logger.debug("select query is {}", query);
        if(selectParameters.isEmpty()) {

            return jdbcClient.rxQuery(query)
                    .flatMap(resultSet -> {
                        if(resultSet.getRows() == null || resultSet.getRows().isEmpty()) {
                            return Single.just(new JsonObject());
                        }
                        return Single.just(resultSet.getRows(true).get(0));

                    });

        }


        return jdbcClient.rxQueryWithParams(selectBuilder.toString(), selectParameters)
                .flatMap(resultSet -> {
                    if(resultSet.getRows() == null || resultSet.getRows().isEmpty()) {
                        return Single.just(new JsonObject());
                    }
                    return Single.just(resultSet.getRows(true).get(0));

                });


    }

   public <T> Single<JsonArray> runSingleColumnQuery(String column, JsonObject params) {

        if(params.isEmpty())
            return Single.error(new DBQueryException(400, "missing parameters"));

        if(params.getString("table") == null || params.getString("table").isEmpty())
            return Single.error(new DBQueryException(400, "missing parameters"));


        SelectBuilder sb = new SelectBuilder()
               .distinct()
               .column(column)
               .from(params.getString("table"));


       if(params.getJsonObject("criteria") != null && !params.getJsonObject("criteria").isEmpty()) {

           JsonObject criteria = params.getJsonObject("criteria");
           String[] keys = criteria.fieldNames().toArray(new String[]{});

           for (int i = 0; i < keys.length; i++) {
               sb.where(keys[i] + " = " + sqlize(criteria.getValue(keys[i])));
           }

       }

       applySort(sb, column, params.getJsonObject("sort", new JsonObject()).getString("order", "asc"), params.getString("table"));

       logger.debug("query is {}", sb.toString());

       return jdbcClient.rxQuery(sb.toString())
               .flatMap(rs -> {

                   List<T> result = rs.getRows().stream()
                           .<T>map((o) -> (T) o.getValue(column, o.getValue(column.toUpperCase())))
                           .collect(Collectors.toList());

                   return Single.just(new JsonArray(result));
               });


   }

   private String sqlize(Object value) {

        if(value instanceof Integer)
            return String.valueOf(value);

        if(value instanceof Double)
            return String.valueOf(value);

        if(value instanceof Long)
            return String.valueOf(value);

        if(value instanceof Float)
            return String.valueOf(value);

        if(value instanceof Boolean)
            return String.valueOf(value);

       return "'" + String.valueOf(value) + "'";

   }

    public Single<JsonObject> runTotalCount(JsonObject data) {

        StringBuilder sb = new StringBuilder(data.getString("countQuery"));
        JsonObject params = data.getJsonObject("params");

        Set<String> keys = new HashSet<String>(params.fieldNames());
        keys.remove("page");
        keys.remove("count");
        JsonArray array = new JsonArray();

        if (keys != null && !keys.isEmpty()) {

            sb.append(" where ");

            for (String key : keys) {

                if (params.getValue(key) == null)
                    continue;

                if ((params.getValue(key) instanceof JsonArray) && params.getJsonArray(key).isEmpty()) {
                    continue;
                }

                if (array.size() > 0) {
                    sb.append(" and ");
                }

                if (params.getValue(key) instanceof JsonArray) {

                    sb.append(key).append(" in (");
                    for (int i = 0; i < params.getJsonArray(key).size(); i++) {
                        sb.append("?");
                        array.add(params.getJsonArray(key).getValue(i));
                        if (params.getJsonArray(key).size() > 0 && i < params.getJsonArray(key).size() - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append(") ");

                } else if(params.containsKey("from")) {

                    sb.append(key).append(" >= ? ");
                    array.add(params.getString(key));

                } else if  (params.containsKey("to")) {

                    sb.append(key).append(" <= ? ");
                    array.add(params.getString(key));

                } else {
                    sb.append(key).append(" = ? ");
                    array.add(params.getValue(key));
                }
            }

            logger.debug("query is {}", sb.toString());
            return jdbcClient.rxQueryWithParams(sb.toString(), array)
                    .flatMap(resultSet -> {
                        JsonObject result = resultSet.getRows().get(0);
                        int total = result.getInteger("total", result.getInteger("TOTAL"));
                        data.put("total", total);
                        return Single.just(data);
                    });


        } else {

            logger.debug("query is {}", sb.toString());

            return jdbcClient.rxQuery(sb.toString())
                    .flatMap(resultSet -> {

                        JsonObject result = resultSet.getRows().get(0);
                        int total = result.getInteger("total", result.getInteger("TOTAL"));

                        data.put("total", total);
                       return Single.just(data);
                    });


        }


    }

    public Single<Boolean> checkTableEntryExists(JsonObject data) {

        JsonObject criteria = data.getJsonObject("checkCriteria");
        String checkField = data.getString("checkField");
        String table = data.getString("checkTable");


        if (table == null || table.isEmpty()) {
            return Single.error(new DBQueryException(500, "check table not specified"));
        }

        if (criteria == null || criteria.isEmpty()) {
            return Single.just(false);
        }

        if (checkField == null || checkField.isEmpty()) {
            return Single.just(false);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("select count(").append(checkField).append(") num ").append("from ").append(table).append(" where ");

        Set<String> keyset = criteria.fieldNames();
        JsonArray array = new JsonArray();

        for (String key : keyset) {

            if (array.size() > 0) {
                sb.append(" and ");
            }

            sb.append(key).append(" = ? ");
            array.add(criteria.getValue(key));

        }

        logger.debug("check query is {}", sb.toString());
        return jdbcClient.rxQueryWithParams(sb.toString(), array)
                .flatMap(resultSet -> {
                   if(resultSet.getRows(true).isEmpty()) {
                       return Single.just(false);
                   }
                    JsonObject response = resultSet.getRows(true).get(0);

                    if (response.getInteger("num", response.getInteger("NUM")) < 1) {
                        logger.debug("check result is {}", response.encodePrettily());
                        logger.debug("num is less than 1");
                        return Single.just(false);
                    }
                    return Single.just(true);
                });



    }

    public Single<JsonObject> insertIntoTable(JsonObject data) {

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ")
                .append(data.getString("table")).append(" (");

        JsonObject payload = data.getJsonObject("payload");

        Set<String> keyset = payload.fieldNames();
        JsonArray array = new JsonArray();

        for (String key : keyset) {

            if (array.size() > 0)
                sb.append(", ");

            sb.append(key);

            array.add(payload.getValue(key));

        }
        sb.append(") values (");

        List<String> vals = array.getList();
        for (int i = 0; i < vals.size(); i++) {

            if (i > 0)
                sb.append(", ");

            sb.append("?");
        }


        sb.append(");");
        logger.debug("insert query is {}", sb.toString());
        return jdbcClient.rxUpdateWithParams(sb.toString(), array)
                .flatMap(ur -> {
                    logger.debug("insert into table completed, {}", ur.getUpdated());
                    return Single.just(new JsonObject().put("success", ur.getUpdated() > 0));
                });

    }

    public Observable<JsonObject> getObservableQueryResults(JsonObject queryObject) {

        Observable<JsonObject> observable = Observable.create(emitter -> {

            JsonObject criteria = queryObject.getJsonObject("criteria", new JsonObject());
            JsonArray columns = queryObject.getJsonArray("columns");

            String tableAlias = queryObject.getString("table");

            SelectBuilder selectBuilder = new SelectBuilder();
            selectBuilder.from(queryObject.getString("table") + " " + tableAlias);

            JsonArray selectParameters = new JsonArray();

            for (int i = 0; i < columns.size(); i++) {
                selectBuilder.column( tableAlias + "." + columns.getString(i));
            }

            if(queryObject.containsKey("joins") && queryObject.getJsonArray("joins") != null) {
                JsonArray joins = queryObject.getJsonArray("joins");
                if(joins.size() > 10)
                    throw new DBQueryException(500, "10 is the maximum number of joins allowed");

                for (int i = 0; i < joins.size(); i++) {

                    addJoin(selectBuilder, tableAlias,joins.getJsonObject(i),0, true, selectParameters);

                }

            }

            if(queryObject.containsKey("sort") && queryObject.getJsonObject("sort") != null) {

                JsonObject sort = queryObject.getJsonObject("sort");

                if(sort.getString("by") == null)
                    throw new DBQueryException(500, "sort.by value is null");

                selectBuilder.orderBy(tableAlias + "." + sort.getString("by"), sort.getString("order", "desc").equalsIgnoreCase("asc"));

            }

            addCriteria(selectBuilder, criteria, tableAlias, selectParameters);

            if(queryObject.containsKey("distinct") && queryObject.getBoolean("distinct")) {
                selectBuilder.distinct();
            }

            String query = new StringBuilder(selectBuilder.toString()).toString();
            logger.debug("select query is {}", query);

            if(selectParameters != null && selectParameters.isEmpty()) {

                jdbcClient.rxQueryStream(query)
                        .toObservable()
                        .flatMap(sqlRowStream -> {
                            return sqlRowStream.toObservable()
                                    .flatMap(row -> {

                                        JsonObject object = new JsonObject();
                                        for (int i = 0; i < columns.size(); i++) {
                                            object.put(columns.getString(i), row.getValue(i));
                                        }
                                        return Observable.just(object);
                                    });

                        })
                        .doOnError(throwable -> emitter.onError(throwable))
                        .doOnComplete(() -> emitter.onComplete())
                        .doOnNext(row -> emitter.onNext(row))
                        .subscribe();

            } else {

                jdbcClient.rxQueryStreamWithParams(query, selectParameters)
                        .toObservable()
                        .flatMap(sqlRowStream -> {
                            return sqlRowStream.toObservable()
                                    .flatMap(row -> {

                                        JsonObject object = new JsonObject();
                                        for (int i = 0; i < columns.size(); i++) {
                                            object.put(columns.getString(i), row.getValue(i));
                                        }
                                        return Observable.just(object);
                                    });

                        })
                        .doOnError(throwable -> emitter.onError(throwable))
                        .doOnComplete(() -> emitter.onComplete())
                        .doOnNext(row -> emitter.onNext(row))
                        .subscribe();

            }

        });

        return observable;

    }

}
