package org.geekhub.storage;

import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;
import org.geekhub.objects.User;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

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
        try (Statement statement = connection.createStatement()){
            List<T> result = extractResult(clazz,statement.executeQuery(sql));
            return result;
        }
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        String sql = "DELETE FROM " + entity.getClass().getSimpleName() + " WHERE id=?";
        int result = 0;
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setInt(1,entity.getId());
            result = statement.executeUpdate();
            return result > 0 ? true : false;
        }
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);

        String sql = null;
        StringBuilder sqlBuild = new StringBuilder();
        if (entity.isNew()) {
            StringJoiner fieldNames = new StringJoiner(",","(",")");
            StringJoiner values = new StringJoiner(",","(",")");
            sqlBuild.append("INSERT INTO " + entity.getClass().getSimpleName());
            for (String fieldName : data.keySet()) {
                fieldNames.add(fieldName);
                values.add("?");
            }

            sqlBuild.append(fieldNames.toString());
            sqlBuild.append(" VALUES "+values.toString());
            sql = sqlBuild.toString();

        } else {
            StringJoiner values = new StringJoiner(",");
            sqlBuild.append("UPDATE "+ entity.getClass().getSimpleName() +" SET ");
            for (String fieldName : data.keySet()) {
                values.add(fieldName + "=?");
            }
            sqlBuild.append(values.toString());
            sqlBuild.append(" WHERE id =" + entity.getId());
            sql = sqlBuild.toString();
        }

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            int i = 1;
            for (String fieldName: data.keySet()){
                statement.setObject(i++, data.get(fieldName));
            }
            statement.executeUpdate();
        }
    }

    //converts object to map, could be helpful in save method
    private static <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Field [] superClassFields = entity.getClass().getSuperclass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(Arrays.asList(superClassFields));
        fields.addAll(Arrays.asList(entity.getClass().getDeclaredFields()));

        Map<String,Object> entityMap = new HashMap<>();
        for (Field field:fields) {
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Ignore.class)) {
                entityMap.put(field.getName(),field.get(entity));
            }
        }

        return entityMap;
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        Field [] superClassFields = clazz.getSuperclass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(Arrays.asList(superClassFields));
        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        List<T> result = new ArrayList<>();
        while (resultset.next()) {

            T entity = clazz.newInstance();
            for(Field field: fields) {

                if (!field.isAnnotationPresent(Ignore.class)) {
                    field.setAccessible(true);
                    field.set(entity, resultset.getObject(field.getName()));
                }
            }
            result.add(entity);
        }
        return result;
    }

}
