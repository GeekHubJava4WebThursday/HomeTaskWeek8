package org.geekhub.storage;

import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;

import java.lang.reflect.Field;
import java.sql.*;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

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
        String sql = "SELECT * FROM " + clazz.getSimpleName() + " WHERE id = " + id;
        try(Statement statement = connection.createStatement()) {
            List<T> result = extractResult(clazz, statement.executeQuery(sql));
            return result.isEmpty() ? null : result.get(0);
        }
    }

    @Override
    public <T extends Entity> List<T> list(Class<T> clazz) throws Exception {
        String sql = "SELECT * FROM " + clazz.getSimpleName();
        try (Statement statement = connection.createStatement()) {
            List<T> result = extractResult(clazz, statement.executeQuery(sql));
            return result.isEmpty() ? Collections.<T>emptyList() : result;
        }
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        String sql = "DELETE FROM " + entity.getClass().getSimpleName() + " WHERE id=" + entity.getId();
        try (Statement statement = connection.createStatement()) {
            return 0 != statement.executeUpdate(sql);
        }
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);

        String sql = null;
        if (entity.isNew()) {
            StringBuilder columnName = new StringBuilder();
            StringBuilder values = new StringBuilder();

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                columnName.append(entry.getKey()).append(", ");
                values.append("'").append(entry.getValue()).append("', ");
            }

            columnName.setLength(columnName.length() - 2);
            values.setLength(values.length() - 2);

            sql = "INSERT INTO " + entity.getClass().getSimpleName() + " ( " + columnName + " ) VALUES " + " ( " + values + " ) ";

        } else {
            sql = "UPDATE " + entity.getClass().getSimpleName() + " SET ";

            for (Map.Entry<String, Object> entry : data.entrySet()) {
               sql += entry.getKey() + " = '" + entry.getValue().toString() + "', ";
            }

            sql = sql.substring(0, sql.length() - 2);
            sql += " WHERE id = " + entity.getId();
        }

        try (Statement statement  = connection.createStatement()) {
            statement.executeUpdate(sql);

            if (entity.isNew()) {
                ResultSet resultSet = statement.getGeneratedKeys();
                resultSet.next();
                entity.setId(resultSet.getInt(1));
            }
        }
    }

    //converts object to map, could be helpful in save method
    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String, Object> data = new HashMap<>();
        Field[] fields = entity.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Ignore.class)) {
                if (field.getType().equals(Boolean.class)) {
                    data.put(field.getName(), (Boolean)field.get(entity) ? 1 : 0);
                } else {
                    data.put(field.getName(), field.get(entity));
                }
            }
        }

        return data;
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        List<T> data = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();

        while (resultset.next()) {
            T entity = clazz.newInstance();
            entity.setId(resultset.getInt("id"));

            for (Field field : fields) {
                field.setAccessible(true);
                if (!field.isAnnotationPresent(Ignore.class)) {
                    field.set(entity, resultset.getObject(field.getName()));
                }
            }

            data.add(entity);
        }

        return data;
    }
}
