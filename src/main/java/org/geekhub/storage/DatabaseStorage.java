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
            return extractResult(clazz, statement.executeQuery(sql));
        }
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        Integer id = entity.getId();
        String tableName = entity.getClass().getSimpleName();
        String sql = "DELETE FROM " + tableName + " WHERE id=" + id;
        return (connection.createStatement().executeUpdate(sql) == 1);
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Field[] fields = getAllDeclaredFields(entity.getClass());
        String tableName = entity.getClass().getSimpleName();
        String sql;
        if (entity.isNew()) {
            List<String> columnsList = new ArrayList<>(fields.length);
            List<String> valuesList = new ArrayList<>(fields.length);
            for (Field field : fields) {
                if (field.getName().equals("id")) {
                    continue;
                }
                columnsList.add(field.getName());
                valuesList.add(convertToSqlType(field.get(entity)));
            }
            String columns = String.join(",", columnsList);
            String values = String.join(",", valuesList);
            sql = "INSERT INTO " + tableName + " (" + columns + ") " + "VALUES" + " (" + values + ")";
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            try (ResultSet result = statement.getGeneratedKeys()) {
                if (result.next()) {
                    entity.setId(result.getInt(1));
                }
            }
        } else {
            List<String> updatesList = new ArrayList<>(fields.length);
            for (Field field : fields) {
                updatesList.add(field.getName() + "=" + convertToSqlType(field.get(entity)));
            }
            String updates = String.join(",", updatesList);
            sql = "UPDATE " + tableName + " SET " + updates + " WHERE id = " + entity.getId();
            connection.createStatement().executeUpdate(sql);
        }
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultSet) throws Exception {
        List<T> result = new ArrayList<>();

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
            T entity = clazz.newInstance();
            columns:
            for (int column = 1; column <= resultSetMetaData.getColumnCount(); column++) {
                String fieldName = resultSetMetaData.getColumnName(column);
                Field[] fields = getAllDeclaredFields(clazz);
                for (Field field : fields) {
                    if (fieldName.equals(field.getName())) {
                        /* String className = resultSetMetaData.getColumnClassName(column);
                        Class fieldClass = Class.forName(className);
                        Object value = fieldClass.newInstance(); */
                        Object value = resultSet.getObject(column);
                        field.set(entity, value);
                        continue columns;
                    }
                }
                throw new RuntimeException("Cannot map record to class' field");
            }
            result.add(entity);
        }
        return result;
    }

    /**
     * Gets all declared fields from class and its ancestors.
     * Set access true to all returned fields
     * Synthetic fields and fields with {@code @Ignore} annotation do not include
     * @param clazz entity class
     * @return array of all fields except fields with @Ignore annotation and synthetic fields
     */
    protected Field[] getAllDeclaredFields(Class clazz) {
        List<Field> allFields = new ArrayList<>();

        Class entityClass = clazz;
        do {
            Field[] fields = entityClass.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.isAnnotationPresent(Ignore.class) || field.isSynthetic()) {
                    continue;
                }
                allFields.add(field);
            }
        } while ( ! (entityClass = entityClass.getSuperclass()).equals(Object.class) );

        return allFields.toArray(new Field[allFields.size()]);
    }

    private String convertToSqlType(Object value) {
        Class valueClass = value.getClass();
        if (value instanceof Number) {
            return value.toString();
        } else if (valueClass.equals(Boolean.class)) {
            return ((Boolean) value) ? "1" : "0";
        } else { // as string
            return "'" + value.toString() + "'";
        }
    }

}
