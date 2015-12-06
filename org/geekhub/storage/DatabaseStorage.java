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
    private Map<String, Object> dataMap;

    public DatabaseStorage(Connection connection) {
        this.connection = connection;
    }

    @Override
    public <T extends Entity> T get(Class<T> clazz, Integer id) throws Exception {
        //this method is fully implemented, no need to do anything, it's just an example
        String sql = "SELECT * FROM " + clazz.getSimpleName() + " WHERE id = " + id;
        try (Statement statement = connection.createStatement()) {
            List<T> result = extractResult(clazz, statement.executeQuery(sql));
            return result.isEmpty() ? null : result.get(0);
        }
    }

    @Override
    public <T extends Entity> List<T> list(Class<T> clazz) throws Exception {
        String sql = "SELECT * FROM " + clazz.getSimpleName();
        try (Statement statement = connection.createStatement()) {
            return extractResult(clazz, statement.executeQuery(sql));
        }
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        String sql = "DELETE FROM " + entity.getClass().getSimpleName() + " WHERE id = " + entity.getId();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            return true;
        }
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        dataMap = prepareEntity(entity);

        String table = entity.getClass().getSimpleName();

        String parameters = "";
        for (int i = 0; i < dataMap.size(); i++) {
            parameters += "?,";
        }
        parameters = parameters.substring(0, parameters.lastIndexOf(','));

        String sql;
        if (entity.isNew()) {
            sql = "INSERT INTO " + table + "(" + prepareParameters(",") + ") VALUES(" + parameters + ")";
        } else {
            sql = "UPDATE " + table + " SET " + prepareParameters("=?,") + " WHERE id = ?";
            dataMap.put("id", entity.getId());
        }

        int id = sendRequest(sql);
        if (entity.isNew()) {
            entity.setId(id);
        }
    }

    private int sendRequest(String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;
            for (String s : dataMap.keySet()) {
                statement.setObject(i++, dataMap.get(s));
            }
            statement.executeUpdate();

            ResultSet rs = statement.executeQuery("SELECT LAST_INSERT_ID()");
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new SQLException("Result set is empty");
            }
        }
    }

    private String prepareParameters(String addition) {
        String parameters = "";
        for (String s : dataMap.keySet()) {
            parameters += s + addition;
        }
        return parameters.substring(0, parameters.lastIndexOf(','));
    }

    //converts object to map, could be helpful in save method
    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String, Object> objectMap = new LinkedHashMap<>();
        for (Field field : entity.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Ignore.class)) {
                objectMap.put(field.getName(), field.get(entity));
            }
        }
        return objectMap;
    }

    //creates list of new instances of clazz by using data from resultSet
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultSet) throws Exception {
        List<T> result = new ArrayList<>();
        while (resultSet.next()) {
            List<Field> fields = new ArrayList<>();
            Collections.addAll(fields, clazz.getDeclaredFields());
            Collections.addAll(fields, clazz.getSuperclass().getDeclaredFields());

            T entity = clazz.newInstance();
            for (Field field : fields) {
                field.setAccessible(true);
                if (!field.isAnnotationPresent(Ignore.class)) {
                    field.set(entity, resultSet.getObject(field.getName()));
                }
            }
            result.add(entity);
        }
        return result;
    }
}
