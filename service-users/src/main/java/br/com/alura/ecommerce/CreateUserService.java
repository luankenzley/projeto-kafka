package br.com.alura.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CreateUserService {
	
	private final Connection connection;
	private PreparedStatement prepareStatement;

	public CreateUserService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		connection = DriverManager.getConnection(url);
		connection.createStatement().execute("create table Users ("
				 + "uuid varchar(200) primary key," 
		+ "email varchar(200))");
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException, SQLException {
		var createUserService = new CreateUserService();
		try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				createUserService::parse, Order.class, Map.of())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException, SQLException {
		System.out.println("------------------------------------------");
		System.out.println("Processing new order, checking for new user");
		System.out.println(record.value());
		var order = record.value();
		if(isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}
	}

	private void insertNewUser(String email) throws SQLException {
		var insert = connection.prepareStatement("insert into Users (uuid, email)"
				+ "values (?,?");
		
		insert.setString(1, "uuid");
		insert.setString(2, email);
		insert.execute();
		System.out.println("Usuário uuid e " + email + "adicionado");
	}

	private boolean isNewUser(String email) throws SQLException {
		var existis = connection.prepareStatement("select uuid from Users" + "where email = ? limit 1");
		existis.setString(1, email);
		
		var results = existis.executeQuery();
		return !results.next();
	}

}
