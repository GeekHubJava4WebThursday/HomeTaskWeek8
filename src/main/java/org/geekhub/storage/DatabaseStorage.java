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
    private static String DELETE_BY_ID_SQL_QUERY_TEMPLATE = "DELETE FROM ? WHERE id = ?";

    private PreparedStatement deleteStatement;

    private Connection connection;

    public DatabaseStorage(Connection connection) {
        this.connection = connection;
        try {
            deleteStatement = connection.prepareStatement(DELETE_BY_ID_SQL_QUERY_TEMPLATE);
        } catch (SQLException ignore) { }
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
        deleteStatement.setString(1, entity.getClass().getSimpleName());
        deleteStatement.setInt(2, id);
        return deleteStatement.executeUpdate() == 1;
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);

        String sql = null;
        if (entity.isNew()) {
            //implement me
            //need to define right SQL query to create object
        } else {
            //implement me
            //need to define right SQL query to update object
        }

        //implement me, need to save/update object and update it with new id if it's a creation
    }

    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String,Object> allFields = new HashMap<>();

        Class entityClass = entity.getClass();
        do {
            Field[] fields = entityClass.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.isAnnotationPresent(Ignore.class) || field.isSynthetic()) {
                    continue;
                }
                String fieldName = field.getName();
                Object fieldValue = field.get(entity);
                allFields.put(fieldName, fieldValue);
            }
        } while ( ! (entityClass = entityClass.getSuperclass()).equals(Object.class) );
System.out.println(allFields); // TODO delete debug code
        return allFields;
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
                Class entityClass = clazz;
                do {
                    Field[] fields = entityClass.getDeclaredFields();
                    for (Field field : fields) {
                        if (fieldName.equals(field.getName())) {
                            field.setAccessible(true); // TODO move up if access exception
                            /* String className = resultSetMetaData.getColumnClassName(column);
                            Class fieldClass = Class.forName(className);
                            Object value = fieldClass.newInstance(); */
                            Object value = resultSet.getObject(column);
                            field.set(entity, value);
                            continue columns;
                        }
                    }
                } while ( ! (entityClass = entityClass.getSuperclass()).equals(Object.class));
                throw new RuntimeException("Cannot map record to class' field");
            }
            result.add(entity);
        }
        return result;
    }
}
