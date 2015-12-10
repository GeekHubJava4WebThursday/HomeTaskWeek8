package org.geekhub;

import org.geekhub.objects.Cat;
import org.geekhub.objects.Entity;
import org.geekhub.objects.Planet;
import org.geekhub.objects.User;
import org.geekhub.storage.DatabaseStorage;
import org.geekhub.storage.Storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        Connection connection = createConnection("root", "root", "geekdb");

        Storage storage = new DatabaseStorage(connection);
        List<Cat> cats = storage.list(Cat.class);
        for (Cat cat : cats) {
            storage.delete(cat);
        }
        cats = storage.list(Cat.class);
        if (!cats.isEmpty()) throw new Exception("Cats should not be in database!");

        for (int i = 1; i <= 20; i++) {
            Cat cat = new Cat();
            cat.setName("cat" + i);
            cat.setAge(i);
            storage.save(cat);
        }

        cats = storage.list(Cat.class);
        if (cats.size() != 20) throw new Exception("Number of cats in storage should be 20!");

        User user = new User();
        user.setAdmin(true);
        user.setAge(23);
        user.setName("Victor");
        user.setBalance(22.23);
        storage.save(user);

        User user1;
        user1 = storage.get(User.class, 46);
        if (user1 != null) {
            if (!user1.getName().equals(user.getName())) throw new Exception("Users should be equals!");
            user = user1;
            user.setAdmin(false);
            storage.save(user);
        }

        User user2 = storage.get(User.class, user.getId());
        if(user2 != null) {
            if (!user.getAdmin().equals(user2.getAdmin())) throw new Exception("Users should be updated!");
            storage.delete(user1);
        }

        User user3 = storage.get(User.class, user.getId());
        if (user3 != null) throw new Exception("User should be deleted!");

        testMyEntity(storage);

        connection.close();
    }

    private static Connection createConnection(String login, String password, String dbName) throws Exception {
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/" + dbName, login, password);
    }

    public static void testMyEntity(Storage storage) throws Exception {
        Planet planet = new Planet();
        planet.setName("Saturn");
        planet.setSatelites(62);
        planet.setType("a gas giant");
        planet.setWeight((long) 52001.4);
        storage.save(planet);

        Planet planet1 = storage.get(Planet.class, 3);
        if(planet1 != null) {
            if (!planet1.getName().equals(planet.getName())) throw new Exception("Planet should be equals!");
            planet = planet1;
            planet.setName("Earth");
            planet.setType("earth category");
            storage.save(planet);
        }
        Planet planet2 = storage.get(Planet.class, 4);
        if(planet2 != null) {
            storage.delete(planet2);
        }
    }

}
