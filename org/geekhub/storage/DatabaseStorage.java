package org.geekhub.storage;

import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link org.geekhub.storage.Storage} that uses database as a storage for objects.
 * It uses simple object type names to define target table to save the object.
 * It uses reflection to access objects fields and retrieve data to map to database tables.
 * As an identifier it uses field id of {@link org.geekhub.objects.Entity} class.
 * Could be created only with {@link java.sql.Connection} specified.
 */
public class DatabaseStorage implements Storage {
    private Connection connection;

    public DatabaseStorage(Connection connection) {
        this.connection = connection;
    }

    @Override
    public <T extends Entity> T get(Class<T> clazz, Integer id) throws Exception {
        //this method is fully implemented, no need to do anything, it's just an example
        String sql = "SELECT * FROM " + clazz.getSimpleName().toLowerCase() + " WHERE id = " + id;
        try (Statement statement = connection.createStatement()) {
            List<T> result = extractResult(clazz, statement.executeQuery(sql));
            return result.isEmpty() ? null : result.get(0);
        }
    }

    @Override
    public <T extends Entity> List<T> list(Class<T> clazz) throws Exception {
        try (Statement statement = connection.createStatement()) {
            return extractResult(clazz, statement.executeQuery("SELECT * FROM " + clazz.getSimpleName().toLowerCase()));
        }
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        String sql = "DELETE FROM " + getEntityName(entity) + " WHERE id = " + entity.getId();
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        }
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);
        String sql;

        if (entity.isNew()) {
            StringBuilder value = new StringBuilder();
            StringBuilder column = new StringBuilder();

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                column.append(entry.getKey()).append(",");
                value.append("'").append(entry.getValue()).append("',");
            }

            column.setLength(column.length() - 1);
            value.setLength(value.length() - 1);
            sql = "INSERT INTO " + getEntityName(entity) + " ( " + column + " ) VALUES " + " ( " + value + " ) ";

        } else {
            sql = "UPDATE " + getEntityName(entity) + " SET ";

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql += entry.getKey() + " = '" + entry.getValue().toString() + "',";
            }

            sql = sql.substring(0, sql.length() - 1);
            sql += " WHERE id = " + entity.getId();
        }

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            if (entity.isNew()) {
                ResultSet resultset = statement.getGeneratedKeys();

                if (resultset.next()) {
                    entity.setId(resultset.getInt(1));
                }
            }
        }
    }

    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String, Object> map = new HashMap<>();
        Field[] fields = entity.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Ignore.class)) {
                if (field.getType().getSimpleName().equals("Boolean")) {
                    map.put(field.getName(), (Boolean) field.get(entity) ? 1 : 0);
                } else map.put(field.getName(), field.get(entity));
            }
        }
        return map;
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        ArrayList<T> list = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();

        while (resultset.next()) {
            T instance = clazz.newInstance();
            instance.setId(resultset.getInt("id"));

            for (Field field : fields) {
                field.setAccessible(true);
                if (!field.isAnnotationPresent(Ignore.class)) {
                    field.set(instance, resultset.getObject(field.getName()));
                }
            }
            list.add(instance);
        }
        return list;
    }


    private  <T extends Entity> String getEntityName(T entity){
        return entity.getClass().getSimpleName().toLowerCase();
    }
}
