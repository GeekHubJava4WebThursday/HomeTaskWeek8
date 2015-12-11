package org.geekhub;

import org.geekhub.objects.Cat;
import org.geekhub.objects.Product;
import org.geekhub.objects.User;
import org.geekhub.storage.DatabaseStorage;
import org.geekhub.storage.Storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        Connection connection = createConnection("root", "1107", "geekdb");

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

        User user1 = storage.get(User.class, user.getId());
        if (!user1.getName().equals(user.getName())) throw new Exception("Users should be equals!");

        user.setAdmin(false);
        storage.save(user);

        User user2 = storage.get(User.class, user.getId());
        if (!user.getAdmin().equals(user2.getAdmin())) throw new Exception("Users should be updated!");

        storage.delete(user1);

        User user3 = storage.get(User.class, user.getId());

        if (user3 != null) throw new Exception("User should be deleted!");

        Product product = new Product();
        product.setType("Book");
        product.setName("Thinking in JAVA");
        product.setAmount(5);
        product.setDelivered(LocalDate.now());
        product.setPrice(Integer.MAX_VALUE);


        storage.save(product);
        Product product1 =storage.get(Product.class,product.getId());
        if(!product.getName().equals(product1.getName())) throw  new Exception("Product name should be equals!");



        connection.close();
        System.out.println("Test completed congratulation! !");
    }

    private static Connection createConnection(String login, String password, String dbName) throws Exception {

        return DriverManager.getConnection("jdbc:mysql://localhost:3306/" + dbName, login, password);

    }
}
