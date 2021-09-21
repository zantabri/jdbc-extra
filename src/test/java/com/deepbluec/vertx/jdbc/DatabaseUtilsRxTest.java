package com.deepbluec.vertx.jdbc;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

//@Disabled
@ExtendWith(VertxExtension.class)
public class DatabaseUtilsRxTest {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseUtilsRxTest.class.getName());
    static DatabaseUtilsRx databaseUtils;
    static JDBCClient client;

    @BeforeAll
    public static void up(Vertx vertx, VertxTestContext context) {

        client = JDBCClient.createShared(io.vertx.reactivex.core.Vertx.vertx(), new JsonObject()
                .put("url", "jdbc:hsqldb:mem:test:shutdown=true")
                .put("driver_class", "org.hsqldb.jdbcDriver")
                .put("min_pool_size", 20));


        TestHelper.prepareDatabase(client)
                .subscribe(o -> {

                    DatabaseUtilsRxTest.databaseUtils = new DatabaseUtilsRx(client);
                    logger.debug("prepare database complete");
                    context.completeNow();

                }, throwable -> {
                    logger.error("prepare database complete ", throwable);
                    context.failNow((Throwable) throwable);
                });
    }

    @Test
    public void check(VertxTestContext context) {
        assertNotNull(databaseUtils);
        context.completeNow();
    }

    @Test
    public void testUpdateRecord(VertxTestContext context) {

        JsonObject jsonObject = new JsonObject()
                .put("table", "foodOrigin")
                .put("payload", new JsonObject()
                        .put("foodOriginTitle", "Lagos, Nigeria")
                ).put("criteria", new JsonObject()
                        .put("foodOriginId", "food-origin-id-1"));

        databaseUtils.updateRecord(jsonObject)
                .subscribe((entries, throwable) -> {
                    if (throwable != null) {
                        logger.error("unexpected error", throwable);
                        context.failNow(throwable);
                        return;
                    }

                    assertNotNull(entries);
                    logger.debug("response is {}", entries.encodePrettily());
                    context.completeNow();

                });

    }


    @Test
    public void testRunPaginationQuery(VertxTestContext context) {

        JsonObject queryObject = new JsonObject().put("table", "recipes")
                .put("page", "1")
                .put("count", "3")
                .put("countField", "recipeId")
                .put("criteria", new JsonObject())
                .put("sort", new JsonObject().put("by", "dateCreated").put("order", "desc"))
                .put("joins", new JsonArray().add(
                                new JsonObject()
                                        .put("table", "foodOrigin")
                                        .put("on", "foodOriginId")
                                        .put("select", new JsonArray().add("foodOriginTitle"))
                                        .put("type", "inner")
                        )
                        .add(new JsonObject()
                                .put("table", "foodCategory")
                                .put("on", "foodCategoryId")
                                .put("type", "inner")
                                .put("select", new JsonArray().add("foodCategoryTitle"))))

                .put("columns", new JsonArray().add("recipeId").add("name").add("description").add("imageUrl").add("difficulty").add("dateCreated"));

        databaseUtils.runPaginationQuery(queryObject)
                .subscribe(o -> {

                    assertNotNull(o);
                    logger.debug("response is {}", o.encodePrettily());
                    JsonArray result = o.getJsonArray("result");
                    assertFalse(result.isEmpty());
                    context.completeNow();

                }, t -> {
                    logger.error("unexpected error ", t);
                    context.failNow(t);
                });

    }

    @Disabled("wip")
    @Test
    public void testRunSearch(VertxTestContext context) {

        JsonObject queryObject = new JsonObject().put("table", "recipes")
                .put("criteria", new JsonObject().put("recipeId", "recipe-id-1"))
                .put("sort", new JsonObject().put("by", "dateCreated").put("order", "desc"))
                .put("joins", new JsonArray().add(
                                new JsonObject()
                                        .put("table", "foodOrigin")
                                        .put("on", "foodOriginId")
                                        .put("select", new JsonArray().add("foodOriginTitle"))
                                        .put("type", "inner"))
                        .add(new JsonObject()
                                .put("table", "foodCategory")
                                .put("on", "foodCategoryId")
                                .put("type", "inner")
                                .put("select", new JsonArray().add("foodCategoryTitle"))))
                .put("columns", new JsonArray().add("recipeId").add("name").add("description").add("imageUrl").add("difficulty").add("dateCreated"));
        databaseUtils.runSearch(queryObject)
                .doOnError(context::failNow)
                .doOnSuccess(o -> {
                    context.completeNow();
                }).subscribe();

    }

    @Test
    public void testRunSelectQuery(VertxTestContext context) {

        JsonObject queryObject = new JsonObject().put("table", "recipes")
                .put("criteria", new JsonObject().put("recipeId", "recipe-id-1"))
                .put("sort", new JsonObject().put("by", "dateCreated").put("order", "desc"))
                .put("joins", new JsonArray().add(
                                new JsonObject()
                                        .put("table", "foodOrigin")
                                        .put("on", "foodOriginId")
                                        .put("select", new JsonArray().add("foodOriginTitle"))
                                        .put("type", "inner"))
                        .add(new JsonObject()
                                .put("table", "foodCategory")
                                .put("on", "foodCategoryId")
                                .put("type", "inner")
                                .put("select", new JsonArray().add("foodCategoryTitle"))))
                .put("columns", new JsonArray().add("recipeId").add("name").add("description").add("imageUrl").add("difficulty").add("dateCreated"));
        databaseUtils.runSelectQuery(queryObject)
                .subscribe(o -> {

                    assertNotNull(o);
                    logger.debug("response is {}", o.encodePrettily());
                    assertFalse(o.isEmpty());
                    context.completeNow();

                }, t -> {
                    logger.error("unexpected error ", t);
                    context.failNow(t);
                });

    }

    @Test
    public void testRunSingleRowQuery(VertxTestContext context) {

        JsonObject queryObject = new JsonObject().put("table", "recipes")
                .put("criteria", new JsonObject().put("recipeId", "recipe-id-1"))
                .put("sort", new JsonObject().put("by", "dateCreated").put("order", "desc"))
                .put("joins", new JsonArray().add(
                                new JsonObject()
                                        .put("table", "foodOrigin")
                                        .put("on", "foodOriginId")
                                        .put("select", new JsonArray().add("foodOriginTitle"))
                                        .put("type", "inner")
                        )
                        .add(new JsonObject()
                                .put("table", "foodCategory")
                                .put("on", "foodCategoryId")
                                .put("type", "inner")
                                .put("select", new JsonArray().add("foodCategoryTitle"))))
                .put("columns", new JsonArray().add("recipeId").add("name").add("description").add("imageUrl").add("difficulty").add("dateCreated"));

        databaseUtils.runSingleRowQuery(queryObject)
                .subscribe(o -> {

                    assertNotNull(o);
                    logger.debug("response is {}", o.encodePrettily());
                    assertFalse(o.isEmpty());
                    context.completeNow();

                }, t -> {
                    logger.error("unexpected error ", t);
                    context.failNow(t);
                });

    }


    @Test
    public void testCheckTableEntryExists(VertxTestContext context) {

        JsonObject criteria = new JsonObject().put("recipeId", "recipe-id-1");
        String checkField = "recipeId";
        String table = "recipes";

        JsonObject data = new JsonObject().put("checkCriteria", criteria)
                .put("checkField", checkField)
                .put("checkTable", table);

        databaseUtils.checkTableEntryExists(data)
                .subscribe(o -> {
                    assertNotNull(0);
                    logger.debug("resul is {}", o);
                    context.completeNow();
                }, t -> {
                    context.failNow(t);
                });
    }

    @Test
    public void testInsertIntoTable(VertxTestContext context) {
        String table = "foodOrigin";
        JsonObject payload = new JsonObject()
                .put("foodOriginId", "food-origin-id-2")
                .put("foodOriginTitle", "Accra, Ghana");

        JsonObject data = new JsonObject().put("table", table).put("payload", payload);
        databaseUtils.insertIntoTable(data)
                .subscribe(o -> {
                    assertNotNull(o);
                    logger.debug("response is {}", o.encodePrettily());
                    assertTrue(o.getBoolean("success"));
                    context.completeNow();
                }, t -> {
                    context.failNow(t);
                });
    }

    @Test
    public void testGetObservableQueryResults(VertxTestContext context) {

        JsonObject queryObject = new JsonObject().put("table", "recipes")
                .put("columns", new JsonArray().add("recipeId").add("name").add("description").add("imageUrl").add("difficulty").add("dateCreated"));

        databaseUtils.getObservableQueryResults(queryObject)
                .doOnError(throwable -> context.failNow(throwable))
                .doOnComplete(() -> context.completeNow())
                .doOnNext(row -> {

                    List<String> names = new ArrayList<>(row.fieldNames());
                    StringBuilder sb = new StringBuilder();

                    for (int i = 0; i < names.size(); i++) {
                        sb.append('"').append(row.getValue(names.get(i))).append('"');
                        if (i != names.size() - 1)
                            sb.append(",");
                    }

                    sb.append("\n");
                    logger.debug("entry is {}", sb.toString());

                }).subscribe();

    }

    @Test
    public void testRunSingleColumnQuery(VertxTestContext context) {

        JsonObject queryObject = new JsonObject()
                .put("table", "recipes")
                .put("sort", new JsonObject().put("dateCreated", "desc"))
                .put("criteria", new JsonObject()
                        .put("recipeId", "recipe-id-2"));

        databaseUtils.<String>runSingleColumnQuery("name", queryObject)
                .doOnError(context::failNow)
                .doOnSuccess(a -> {
                    logger.debug("response is {}", a.encodePrettily());
                    assertNotNull(a);
                    assertFalse(a.isEmpty());
                    context.completeNow();
                }).subscribe();
    }


}
