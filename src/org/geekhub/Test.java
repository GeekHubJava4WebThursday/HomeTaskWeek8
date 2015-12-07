package org.geekhub;

import org.geekhub.objects.Car;
import org.geekhub.objects.Cat;
import org.geekhub.objects.User;
import org.geekhub.storage.DatabaseStorage;
import org.geekhub.storage.Storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Random;

public class Test {

    public static final String DBMS     = "mysql";
    public static final String SERVER   = "localhost";
    public static final String USER     = "root";
    public static final String PASSWORD = "admin";
    public static final String BASE     = "geekdb";

    public static void main(String[] args) throws Exception {
        try (Connection connection = createConnection(USER, PASSWORD, BASE)) {
            Storage storage = new DatabaseStorage(connection);

            testCar(storage);

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

            User user1 = storage.get(User.class, user.getId());
            if (!user1.getName().equals(user.getName())) throw new Exception("Users should be equals!");

            user.setAdmin(false);
            storage.save(user);

            User user2 = storage.get(User.class, user.getId());
            if (!user.getAdmin().equals(user2.getAdmin())) throw new Exception("Users should be updated!");

            storage.delete(user1);

            User user3 = storage.get(User.class, user.getId());

            if (user3 != null) throw new Exception("User should be deleted!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void testCar (Storage storage) throws Exception {

        List<Car> cars = storage.list(Car.class);
        for (Car car: cars) {
            System.out.println(car.toString());
        }

        Random random = new Random();
        for(int i = 1; i <= 5; i++) {
            Car car = new Car();
            car.setName("car" + random.nextInt(i * 100));
            car.setMileage(1000 * (int)Math.pow(i,random.nextInt(i)));
            car.setPrice(random.nextDouble() * 10000);
            car.setUsed(random.nextBoolean());
            storage.save(car);
        }

        List<Car> newCars = storage.list(Car.class);

        int usedCar = -1;
        int usedCount = 0;
        int sameCars = 0;
        for (Car newCar: newCars) {
            for (Car car: cars) {
                if (car.getId() == newCar.getId()) {
                    sameCars++;
                    break;
                }
            }
            if (newCar.getUsed()) {
                usedCount ++;
                usedCar = newCar.getId();
            }
        }
        if (sameCars != cars.size()) throw new Exception("Save new exception!");

        for (Car car: newCars) {
            if (car.getUsed()) {
                storage.delete(car);
            }
        }
        if (storage.get(Car.class, usedCar) != null) throw new Exception("Delete exception!");

        cars = storage.list(Car.class);
        if (cars.size() != newCars.size() - usedCount) throw new Exception("Delete all exception!");

        int carId = -1;
        for (Car car: cars) {
            if (car.getMileage() < 2000) {
                car.setPrice(10000.0);
                storage.save(car);
                carId = car.getId();
                break;
            }
        }
        if (storage.get(Car.class, carId).getPrice() != 10000.0) throw new Exception("Update exception!");
    }

    private static Connection createConnection(String login, String password, String dbName) throws Exception {
        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        String url = "jdbc:" + DBMS +"://" + SERVER + "/" + dbName;
        Connection connection = DriverManager.getConnection(url, login, password);
        return connection;
    }
}
