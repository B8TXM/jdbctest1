package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.*;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {
    }

    @Override
    public void createUsersTable() {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "name VARCHAR(255), " +
                "lastName VARCHAR(255), " +
                "age TINYINT" +
                ")";
        try (Connection con = Util.getConnection();
             Statement statement = con.createStatement()) {
            statement.execute(sql);
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Ошибка при создании таблицы: " + e.getMessage());
        }
    }

    @Override
    public void dropUsersTable() {
        String sql = "DROP TABLE IF EXISTS users";
        try (Connection con = Util.getConnection();
             Statement statement = con.createStatement()) {
            statement.execute(sql);
            System.out.println("Таблица users успешно удалена.");
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Ошибка при удалении таблицы: " + e.getMessage());
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";
        try (Connection con = Util.getConnection()) {
            con.setAutoCommit(false);

            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setString(1, name);
                ps.setString(2, lastName);
                ps.setByte(3, age);
                int rowsAffected = ps.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.printf("User с именем — %s добавлен в базу данных%n", name);
                }
                con.commit();
            } catch (SQLException e) {
                con.rollback();
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Ошибка при сохранении пользователя: " + e.getMessage());
        }
    }

    @Override
    public void removeUserById(long id) {
        String sql = "DELETE FROM users WHERE id = ?";
        try (Connection con = Util.getConnection()) {
            con.setAutoCommit(false);

            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setLong(1, id);
                int rowsAffected = ps.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Пользователь с ID " + id + " удалён.");
                } else {
                    System.out.println("Пользователь с ID " + id + " не найден.");
                }
                con.commit();
            } catch (SQLException e) {
                con.rollback();
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Ошибка при удалении пользователя: " + e.getMessage());
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String sql = "SELECT id, name, lastName, age FROM users";
        try (Connection con = Util.getConnection();
             Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong("id"));
                user.setName(rs.getString("name"));
                user.setLastName(rs.getString("lastName"));
                user.setAge(rs.getByte("age"));
                users.add(user);
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Ошибка при получении списка пользователей: " + e.getMessage());
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        String sql = "DELETE FROM users";
        try (Connection con = Util.getConnection()) {
            con.setAutoCommit(false);

            try(Statement statement = con.createStatement()) {
                int rowsDeleted = statement.executeUpdate(sql);
                con.commit();
                System.out.println("Очищено записей: " + rowsDeleted);
            } catch (SQLException e) {
                con.rollback();
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Ошибка при очистке таблицы: " + e.getMessage());
        }
    }
}