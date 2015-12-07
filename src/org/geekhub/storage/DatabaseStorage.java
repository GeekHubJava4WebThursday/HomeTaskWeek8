package org.geekhub.storage;

import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
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
        String sql = "SELECT * FROM " + clazz.getSimpleName() + " WHERE id = " + id;
        try(Statement statement = connection.createStatement()) {
            List<T> result = extractResult(clazz, statement.executeQuery(sql));
            return result.isEmpty() ? null : result.get(0);
        }
    }

    @Override
    public <T extends Entity> List<T> list(Class<T> clazz) throws Exception {
        String sql = "SELECT * FROM " + clazz.getSimpleName();
        try(Statement statement = connection.createStatement()) {
            return extractResult(clazz, statement.executeQuery(sql));
        }
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        if (entity.getId() == null) {
            return false;
        }
        String sql = "DELETE FROM " + entity.getClass().getSimpleName() + " WHERE id = " + entity.getId();
        try(Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql) > 0;
        }
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);
        String sql;
        if (entity.isNew()) {
            sql = formInsertSql(data, entity);
        } else {
            sql = formUpdateSql(data, entity);
        }
        try(PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            int key = 1;
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                preparedStatement.setObject(key++, entry.getValue());
            }
            preparedStatement.executeUpdate();

            if (entity.isNew()) {
                ResultSet resultSet = preparedStatement.getGeneratedKeys();
                if (resultSet.next()) {
                    entity.setId(resultSet.getInt(1));
                } else {
                    throw new Exception("Error retrieving generated key!");
                }
            }
        }
    }

    // forms insert query for prepareStatement, used in save method
    private <T extends Entity> String formInsertSql(Map<String, Object> data, T entity) throws Exception{
        String sql = "INSERT INTO " + entity.getClass().getSimpleName() + " (";
        for (Map.Entry<String, Object> entry: data.entrySet()) {
            sql += entry.getKey() + ",";
        }
        sql = sql.substring(0, sql.length() - 1) + ") VALUES (";
        for (int i = 0; i < data.size(); i++) {
            sql += "?,";
        }
        return sql.substring(0, sql.length() - 1) + ")";
    }

    // forms update query for prepareStatement, used in save method
    private <T extends Entity> String formUpdateSql(Map<String, Object> data, T entity) throws Exception{
        String sql = "UPDATE " + entity.getClass().getSimpleName() + " SET ";
        for (Map.Entry<String, Object> entry: data.entrySet()) {
            sql += entry.getKey() + " = ?,";
        }
        return sql.substring(0, sql.length() - 1) + " WHERE id = " + entity.getId();
    }

    //converts object to map, could be helpful in save method
    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Class clazz = entity.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Object> row = new HashMap<>(fields.length);
        for (Field field: fields) {
            if (!field.isAnnotationPresent(Ignore.class)) {
                field.setAccessible(true);
                row.put(field.getName(), field.get(entity));
            }
        }
        return row;
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        List<T> list = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        while(resultset.next()) {
            T instance = clazz.newInstance();
            instance.setId(resultset.getInt("id"));
            for (Field field: fields) {
                if (!field.isAnnotationPresent(Ignore.class)) {
                    field.setAccessible(true);
                    field.set(instance, resultset.getObject(field.getName()));
                }
            }
            list.add(instance);
        }
        return list;
    }
}
