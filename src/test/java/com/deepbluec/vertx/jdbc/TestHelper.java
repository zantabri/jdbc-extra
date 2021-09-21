package com.deepbluec.vertx.jdbc;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.jdbc.JDBCClient;

/**
 *
 */
public class TestHelper {

    static final String BUILD_SCHEMA = "drop table if exists recipe;\n" +
            "drop table if exists preparationSteps;\n" +
            "drop table if exists foodOrigin;\n" +
            "drop table if exists foodCategory;\n" +
            "" +
            "create table recipes (\n" +
            "  recipeId varchar(100) not null unique,\n" +
            "  name varchar(100) not null,\n" +
            "  foodCategoryId varchar(100) not null,\n" +
            "  description varchar(250),\n" +
            "  imageUrl varchar(300) not null,\n" +
            "  foodOriginId varchar(250),\n" +
            "  difficulty int not null,\n" +
            "  dateCreated timestamp default current_timestamp\n" +
            ");\n" +
            "create table preparationSteps (\n" +
            "    recipeId varchar(100) not null,\n" +
            "    sequence int not null,\n" +
            "    preparationTitle varchar(250),\n" +
            "    content varchar(300)\n" +
            ");\n" +
            "create table foodOrigin (\n" +
            "    foodOriginId varchar(256) not null unique,\n" +
            "    foodOriginTitle varchar(250) not null unique\n" +
            ");\n" +
            "create table foodCategory (\n" +
            "    foodCategoryId varchar(256) not null unique,\n" +
            "    foodCategoryTitle varchar(250) not null unique\n" +
            ");\n";

    static final String INSERT_TEST_DATA = "insert into recipes (recipeId, name, foodCategoryId, description, foodOriginId, difficulty, imageUrl, dateCreated) " +
            "values ('recipe-id-1', 'jollof', 'food-category-id-1', 'description', 'food-origin-id-1', 4, 'url-1','2020-03-24 13:00:00' ), " +
            "('recipe-id-2', 'fufu', 'food-category-id-2', 'description', 'food-origin-id-1', 3, 'url-2', CURRENT_TIMESTAMP);" +
            "\n" +
            "insert into preparationSteps (recipeId, sequence, preparationTitle, content)" +
            "values('recipe-id-1', 0, 'start', 'start cooking')," +
            "('recipe-id-1', 1, 'finish', 'finish cooking')," +
            "('recipe-id-2', 0, 'start', 'start cooking')," +
            "('recipe-id-2', 1, 'finish', 'finish cooking');" +
            "\n" +
            "insert into foodOrigin(foodOriginId, foodOriginTitle) values " +
            "('food-origin-id-1', 'Nigeria');" +
            "\n" +
            "insert into foodCategory(foodCategoryId, foodCategoryTitle) values " +
            "('food-category-id-1', 'Party food')," +
            "('food-category-id-2', 'Soul Food');";
;

    public static Single prepareDatabase(JDBCClient client) {

        return client.rxGetConnection()
                .flatMap(sqlConnection -> sqlConnection.rxExecute(BUILD_SCHEMA).toSingle(() -> sqlConnection))
                .flatMap(sqlConnection -> sqlConnection.rxExecute(INSERT_TEST_DATA).toSingle(() -> sqlConnection))
                .flatMap(sqlConnection -> {
                    sqlConnection.close();
                    return Single.just(new JsonObject().put("success", true));
                });

    }

}
