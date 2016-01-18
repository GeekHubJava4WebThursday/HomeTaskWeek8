package org.geekhub.storage;

import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;

import java.lang.reflect.Field;
import java.sql.*;
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
        //this method is fully implemented, no need to do anything, it's just an example

        String sql = "SELECT * FROM " + clazz.getSimpleName() + " WHERE id = " + id;
        try(Statement statement = connection.createStatement()) {
            List<T> result = extractResult(clazz, statement.executeQuery(sql));
            return result.isEmpty() ? null : result.get(0);
        }
    }

    // done
    @Override
    public <T extends Entity> List<T> list(Class<T> clazz) throws Exception {
        //implement me according to interface by using extractResult method
        try (Statement statement = connection.createStatement()) {
            String sql = "SELECT * FROM " + clazz.getSimpleName();
            return extractResult(clazz, statement.executeQuery(sql));
        }
    }

    // done
    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        try (Statement statement = connection.createStatement()) {
            String sql = "DELETE FROM " + entity.getClass().getSimpleName() + " WHERE id = " + entity.getId();
            int result = statement.executeUpdate(sql);
            return result != 0;
        }
    }

    // ?
    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);
        String sql = "";


        if (entity.isNew()) {
            //implement me
            //need to define right SQL query to create object
            StringBuilder fields = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                fields.append(entry.getKey() + ", ");
                values.append("\'" + entry.getValue() + "\', ");
            }
            fields.replace(fields.length() - 2, fields.length(), "");
            values.replace(values.length() - 2, values.length(), "");
            sql = "INSERT INTO " + entity.getClass().getSimpleName() +
                    "(" + fields.toString() + ") VALUES " +
                    "(" + values.toString() +")";
        } else {
            //implement me
            //need to define right SQL query to update object
            StringBuilder fields = new StringBuilder();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                fields.append(entry.getKey() + " = \'" + entry.getValue() + "\', ");
            }
            fields.replace(fields.length() - 2, fields.length(), "");
            sql = "UPDATE " + entity.getClass().getSimpleName() + " SET " + fields + " WHERE id = " + entity.getId();
        }

        //implement me, need to save/update object and update it with new id if it's a creation
        try (PreparedStatement pStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            pStatement.executeUpdate();
            if (entity.isNew()) {
                ResultSet resultSet = pStatement.getGeneratedKeys();
                if (resultSet.next()) {
                    entity.setId(resultSet.getInt(1));
                }
            }
        }
    }

    // done
    //converts object to map, could be helpful in save method
    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String, Object> prepare = new HashMap<>();
        Field[] fields = entity.getClass().getDeclaredFields();
        for (Field f : fields) {
            f.setAccessible(true);
            if (!f.isAnnotationPresent(Ignore.class)) {
                String name = f.getName();
                if (f.getType().equals(Boolean.class)) {
                    Integer data = 1;
                    if (f.get(entity).equals(false)) {
                        data = 0;
                    }
                    prepare.put(name, data);
                } else {
                    Object data = f.get(entity);
                    prepare.put(name, data);
                }
            }
        }
        return prepare;
    }

    //? creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        List<T> instances = new ArrayList<>();
        while (resultset.next()) {
            T instance = clazz.newInstance();
            instance.setId(resultset.getInt("id"));
            for (Field f : clazz.getDeclaredFields()) {
                f.setAccessible(true);
                if (!f.isAnnotationPresent(Ignore.class)) {
                    String name = f.getName();
                    f.set(instance, resultset.getObject(name));
                }
            }
            instances.add(instance);
        }
        return instances;
    }
}
