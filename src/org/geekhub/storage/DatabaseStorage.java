package org.geekhub.storage;

import org.geekhub.objects.Cat;
import org.geekhub.objects.Entity;
import org.geekhub.objects.Ignore;
import org.geekhub.objects.User;

import java.lang.reflect.*;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.*;
import java.util.Date;

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

    public DatabaseStorage() {

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
        List<T> result = new ArrayList<>();
        try(Statement statement = connection.createStatement()) {
            result = extractResult(clazz, statement.executeQuery(sql));
        }
        return result;
    }

    @Override
    public <T extends Entity> boolean delete(T entity) throws Exception {
        String sql = "delete from " + entity.getClass().getSimpleName() + " where id=" + entity.getId();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            if(statement.executeUpdate() > 0){
                return true;
            }
        }
        return false;
    }

    @Override
    public <T extends Entity> void save(T entity) throws Exception {
        Map<String, Object> data = prepareEntity(entity);
        String sql = null;
        StringBuilder createSql = new StringBuilder();
        if (entity.isNew()) {
            createSql.append("insert into " + entity.getClass().getSimpleName());
            createSql.append(" (");
            for(String colums : data.keySet()){
                createSql.append(colums + ", ");
            }
            createSql.delete(createSql.length()-2, createSql.length());
            createSql.append(") values (");
            for(int i =0; i < data.size(); i++){
                createSql.append("?, ");
            }
            createSql.delete(createSql.length()-2, createSql.length());
            createSql.append(")");
        } else {
            createSql.append("update " + entity.getClass().getSimpleName() + " set ");
            for(String colums : data.keySet()){
                    createSql.append(colums + "=?, ");
            }
            createSql.delete(createSql.length()-2, createSql.length());
            createSql.append(" where id=" + data.get("id"));
        }
        sql = createSql.toString();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int k = 1;
            for (Object value : data.values()) {
                preparedStatement.setObject(k, value);
                k++;
            }
            preparedStatement.executeUpdate();
        }
        //implement me, need to save/update object and update it with new id if it's a creation
    }

    //converts object to map, could be helpful in save method
    private <T extends Entity> Map<String, Object> prepareEntity(T entity) throws Exception {
        Map<String, Object> entityMap = new HashMap<>();
        Class<Entity> entityClassExtends = (Class<Entity>) entity.getClass();
        Class<Entity> entityClass = Entity.class;
        List<Field> fieldList = new ArrayList<>(Arrays.asList(entityClassExtends.getDeclaredFields()));
        fieldList.addAll(Arrays.asList(entityClass.getDeclaredFields()));
        for (Field field : fieldList) {
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Ignore.class)) {
                entityMap.put(field.getName(),field.get(entity));
            }
        }
        return entityMap;
    }

    //creates list of new instances of clazz by using data from resultset
    private <T extends Entity> List<T> extractResult(Class<T> clazz, ResultSet resultset) throws Exception {
        List<T> resultList = new ArrayList<>();
        while (resultset.next()) {
            T entity = clazz.newInstance();
            Class<Entity> superclass = (Class<Entity>) entity.getClass().getSuperclass();
            Field[] fields = superclass.getDeclaredFields();
            for (Field f : fields){
                f.setAccessible(true);
                f.set(entity,resultset.getObject(f.getName()));
            }
            for(Field field : clazz.getDeclaredFields()){
                field.setAccessible(true);
                if(!field.isAnnotationPresent(Ignore.class)){
                    field.set(entity, resultset.getObject(field.getName()));
                }
            }
            resultList.add(entity);
        }
        return resultList;
    }


}
