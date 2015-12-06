package org.geekhub.storage;

import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;

import java.lang.reflect.Field;
import java.sql.Connection;
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
            return statement.executeUpdate(sql) != 0;
        }
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);

        String sql = null;
        StringBuilder query = new StringBuilder();
        if (entity.isNew()) {
            StringBuilder columnNames = new StringBuilder();
            StringBuilder values = new StringBuilder();

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                columnNames.append(entry.getKey()).append(", ");
                values.append("'").append(entry.getValue()).append("', ");
            }

            // we need to remove last comma and space
            columnNames.delete(columnNames.length() - 2, columnNames.length());
            values.delete(values.length() - 2, values.length());

            query.append("INSERT INTO ").append(entity.getClass().getSimpleName())
                    .append(" (").append(columnNames.toString()).append(") ")
                    .append("VALUES (").append(values.toString()).append(")");

        } else {
            query.append("UPDATE ").append(entity.getClass().getSimpleName()).append(" SET ");

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                query.append(entry.getKey()).append(" = '").append(entry.getValue().toString()).append("', ");
            }

            // we need to remove last comma and space
            query.delete(query.length() - 2, query.length());

            query.append(" WHERE id = ").append(entity.getId());
        }

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query.toString(), Statement.RETURN_GENERATED_KEYS);

            if (entity.isNew()) {
                ResultSet resultSet = statement.getGeneratedKeys();
                if (resultSet.next()) {
                    entity.setId(resultSet.getInt(1));
                }
            }
        }
    }

    //converts object to map, could be helpful in save method
    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String, Object> objectMap = new HashMap<>();
        Field[] declaredFields = entity.getClass().getDeclaredFields();

        for (Field field : declaredFields) {
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Ignore.class)) {
                if (field.getType().equals(Boolean.class)) {
                    objectMap.put(field.getName(), (Boolean) field.get(entity) ? 1 : 0);
                } else {
                    objectMap.put(field.getName(), field.get(entity));
                }
            }
        }

        return objectMap;
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        List<T> instancesList = new ArrayList<>();
        Field[] declaredFields = clazz.getDeclaredFields();

        while (resultset.next()) {
            T instance = clazz.newInstance();
            instance.setId(resultset.getInt("id"));

            for (Field field : declaredFields) {
                field.setAccessible(true);
                if (!field.isAnnotationPresent(Ignore.class)) {
                    field.set(instance, resultset.getObject(field.getName()));
                }
            }

            instancesList.add(instance);
        }

        return instancesList;
    }
}
