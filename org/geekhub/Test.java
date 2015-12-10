package org.geekhub;

import org.geekhub.objects.Cat;
import org.geekhub.objects.Person;
import org.geekhub.objects.User;
import org.geekhub.storage.DatabaseStorage;
import org.geekhub.storage.Storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        Connection connection = createConnection("root", "admin", "geekdb");

        Storage storage = new DatabaseStorage(connection);
        List<Cat> cats = storage.list(Cat.class);
        for (Cat cat : cats) {
            storage.delete(cat);
        }
        cats = storage.list(Cat.class);
        if (!cats.isEmpty()) throw new Exception("Cats should not be in database!");

        for(int i = 1; i <= 20; i++) {
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

        List<User> users = storage.list(User.class);
        if (users.isEmpty()) throw new Exception("At least one user should be exist!");
        User user1 = users.get(0);
        if (!user1.getName().equals(user.getName())) throw new Exception("Users should be equals!");
        user.setId(user1.getId());
        user.setAdmin(false);
        storage.save(user);

        User user2 = storage.get(User.class, user.getId());
        if (!user.getAdmin().equals(user2.getAdmin())) throw new Exception("Users should be updated!");

        storage.delete(user1);

        User user3 = storage.get(User.class, user.getId());

        if (user3 != null) throw new Exception("User should be deleted!");

        Person person = new Person();
        person.setName("Alex");
        person.setSurname("Fox");
        person.setPatronymic("I");
        person.setBirthDay(new Date());

        storage.save(person);
        connection.close();
    }

    private static Connection createConnection(String login, String password, String dbName) throws Exception {
        String url = "jdbc:mysql://localhost:3306/"+dbName;
        Connection connection = DriverManager.getConnection(url,login,password);
        return connection;
    }
}
